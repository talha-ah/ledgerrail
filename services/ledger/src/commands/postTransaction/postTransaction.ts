import type { PoolClient } from "pg"

import type { Currency } from "@ledgerrail/money"

import { TOPICS, transactionKey } from "../../kafka/index.js"
import type {
  Event,
  Account,
  AccountBalances,
  TransactionPostedPayload,
} from "../../types.js"

const EVENT_VERSION = 1
const EVENT_TYPE = "TransactionPosted"

interface Posting {
  accountId: number
  amount: bigint // negative or positive
}

export interface PostTransactionInput {
  idempotencyKey: string
  currency: Currency
  postings: Posting[] // >= 2 legs
}

export async function postTransaction(
  input: PostTransactionInput,
  client: PoolClient
): Promise<{ globalSeq: string; eventId: string }> {
  // Lock rows in a deterministic order (ascending account id) so concurrent transactions can't deadlock
  const postings = [...input.postings].sort((a, b) => a.accountId - b.accountId)

  // Shape checks: >= 2 postings, no zero-amount legs
  if (postings.length < 2) {
    throw new Error("Less legs found")
  }
  if (postings.some((p) => p.amount === 0n)) {
    throw new Error("Invalid legs found")
  }

  // INVARIANT: postings.reduce((s,p) => s+p.amount, 0n) === 0n -- else throw
  const totalAmount = postings.reduce((s, p) => s + p.amount, 0n)
  if (totalAmount !== 0n) throw new Error("Invalid transaction amounts")

  // Load the referenced accounts; assert each exists and its currency === input.currency
  const { rows: accounts } = await client.query<Account>(
    `SELECT * FROM accounts WHERE id = ANY($1)`,
    [postings.map((p) => p.accountId)]
  )

  const accountsSet = new Set(accounts.map((a) => a.id))

  if (postings.some((p) => !accountsSet.has(p.accountId))) {
    throw new Error("Invalid Accounts")
  }
  if (accounts.some((a) => a.currency !== input.currency)) {
    throw new Error("Invalid Currency")
  }

  // Overdraft check
  for (const leg of postings) {
    if (leg.amount >= 0n) continue // only debits can overdraw

    const account = accounts.find((a) => a.id === leg.accountId)
    if (!account) throw new Error("Invalid Accounts")

    // System accounts have no balance floor, so there is nothing to check here.
    // Order matters: the type check must precede the lock. The system account
    // participates in every funding transaction, so locking its balance row would
    // serialize the entire ledger on a single row.
    if (account.type === "system") continue

    const { rows: balances } = await client.query<
      Pick<AccountBalances, "pending" | "posted">
    >(
      `SELECT pending, posted FROM account_balances WHERE account_id = $1 FOR UPDATE`,
      [leg.accountId]
    )

    const available = (balances[0]?.posted ?? 0n) + (balances[0]?.pending ?? 0n)
    if (available + leg.amount < 0n) throw new Error("Insufficient funds")
  }

  // INSERT the event (type 'TransactionPosted', v1, payload = postings with amounts as STRINGS)
  const eventPayload = JSON.stringify({
    currency: input.currency,
    idempotencyKey: input.idempotencyKey,
    postings: postings.map((p) => ({
      accountId: p.accountId,
      amount: String(p.amount),
    })),
  } satisfies TransactionPostedPayload)
  const { rows: events } = await client.query<
    Pick<Event, "global_seq" | "event_id" | "created_at" | "payload">
  >(
    `
      INSERT INTO events (event_type, event_version, payload, idempotency_key)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (idempotency_key) DO NOTHING
      RETURNING global_seq, event_id, created_at, payload;
    `,
    [EVENT_TYPE, EVENT_VERSION, eventPayload, input.idempotencyKey]
  )

  // If zero rows, it's a replay, return ids, and do NOT touch balances
  if (!events[0]) {
    const { rows: replayEvent } = await client.query<
      Pick<Event, "global_seq" | "event_id">
    >(`SELECT global_seq, event_id FROM events WHERE idempotency_key = $1`, [
      input.idempotencyKey,
    ])
    if (!replayEvent[0]) {
      throw new Error(
        "A replay was detected but the original isn't visible yet."
      )
    }

    return {
      eventId: replayEvent[0].event_id,
      globalSeq: String(replayEvent[0].global_seq),
    }
  }

  // Otherwise, new event, apply balance updates
  const { event_id: eventId, global_seq: globalSeq } = events[0]

  await client.query(
    `
      INSERT INTO outbox (event_id, global_seq, topic, partition_key)
      VALUES ($1, $2, $3, $4)
    `,
    [eventId, globalSeq, TOPICS.LEDGER_EVENTS, transactionKey(eventId)]
  )

  for (const leg of postings) {
    await client.query(
      `
      INSERT INTO account_balances (account_id, posted)
      VALUES ($1, $2)
      ON CONFLICT (account_id)
      DO UPDATE SET 
        posted = account_balances.posted + EXCLUDED.posted,
        updated_at = NOW()
    `,
      [leg.accountId, leg.amount]
    )
  }

  return { globalSeq: String(globalSeq), eventId }
}
