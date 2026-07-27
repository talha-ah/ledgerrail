import type { PoolClient } from "pg"

import type { Currency } from "@ledgerrail/money"
import type { Account, Balance, Event } from "../types.js"

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

    const { rows: balances } = await client.query<{
      amount: Balance["amount"]
    }>(`SELECT amount FROM balances WHERE account_id = $1 FOR UPDATE;`, [
      leg.accountId,
    ])

    const current = balances[0] ? BigInt(balances[0].amount) : 0n
    if (current + leg.amount < 0n) throw new Error("Insufficient funds")
  }

  // INSERT the event (type 'TransactionPosted', v1, payload = postings with amounts as STRINGS)
  const eventPayload = JSON.stringify({
    currency: input.currency,
    idempotencyKey: input.idempotencyKey,
    postings: postings.map((p) => ({
      accountId: p.accountId,
      amount: String(p.amount),
    })),
  })
  const { rows: events } = await client.query<Event>(
    `
      INSERT INTO events (event_type, event_version, payload, idempotency_key)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (idempotency_key) DO NOTHING
      RETURNING global_seq, event_id;
    `,
    ["TransactionPosted", 1, eventPayload, input.idempotencyKey]
  )

  // If zero rows, it's a replay, return ids, and do NOT touch balances
  if (!events[0]) {
    const { rows: replayEvent } = await client.query<Event>(
      `SELECT global_seq, event_id FROM events WHERE idempotency_key = $1`,
      [input.idempotencyKey]
    )
    if (!replayEvent[0]) {
      throw new Error(
        "A replay was detected but the original isn't visible yet."
      )
    }

    return {
      globalSeq: String(replayEvent[0].global_seq),
      eventId: replayEvent[0].event_id,
    }
  }

  // Otherwise, new event, apply balance updates
  const { global_seq: globalSeq, event_id: eventId } = events[0]

  // For each posting: upsert balances (amount += p.amount, last_event_seq = global_seq)
  for (const leg of postings) {
    await client.query(
      `
        INSERT INTO balances (account_id, amount, last_event_seq)
        VALUES ($1, $2, $3)
        ON CONFLICT (account_id)
        DO UPDATE SET
          amount = balances.amount + EXCLUDED.amount, -- EXCLUDED is the INSERT row conflicted
          last_event_seq = EXCLUDED.last_event_seq;
      `,
      [leg.accountId, leg.amount, globalSeq]
    )
  }

  return { globalSeq: String(globalSeq), eventId }
}
