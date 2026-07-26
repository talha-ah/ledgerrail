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
  // 1. shape checks: >= 2 postings, no zero-amount legs
  if (input.postings.length < 2) {
    throw new Error("Less legs found")
  }
  if (input.postings.some((p) => p.amount === 0n)) {
    throw new Error("Invalid legs found")
  }

  // 2. INVARIANT: postings.reduce((s,p) => s+p.amount, 0n) === 0n -- else throw
  const totalAmount = input.postings.reduce((s, p) => s + p.amount, 0n)
  if (totalAmount !== 0n) throw new Error("Invalid transaction amounts")

  // 3. load the referenced accounts; assert each exists and its currency === input.currency
  const { rows: accounts } = await client.query<Account>(
    `SELECT * FROM accounts WHERE id = ANY($1)`,
    [input.postings.map((p) => p.accountId)]
  )

  const accountsSet = new Set(accounts.map((a) => a.id))

  if (input.postings.some((p) => !accountsSet.has(p.accountId))) {
    throw new Error("Invalid Accounts")
  }
  if (accounts.some((a) => a.currency !== input.currency)) {
    throw new Error("Invalid Currency")
  }

  // 3.1 overdraft check
  for (const leg of input.postings) {
    if (leg.amount >= 0n) continue // only debits can overdraw

    const { rows: balances } = await client.query<Balance>(
      `SELECT amount FROM balances WHERE account_id = $1`,
      [leg.accountId]
    )

    const account = accounts.find((a) => a.id === leg.accountId)
    if (!account) throw new Error("Invalid Accounts")

    if (account.type === "system") continue

    const current = balances[0] ? BigInt(balances[0].amount) : 0n
    if (current + leg.amount < 0n) throw new Error("Insufficient funds")
  }

  // 4. INSERT the event (type 'TransactionPosted', v1, payload = postings with amounts as STRINGS)
  const eventPayload = JSON.stringify({
    currency: input.currency,
    idempotencyKey: input.idempotencyKey,
    postings: input.postings.map((p) => ({
      accountId: p.accountId,
      amount: String(p.amount),
    })),
  })
  const { rows: events } = await client.query<Event>(
    `
      INSERT INTO events (event_type, event_version, payload, idempotency_key)
      VALUES ($1, $2, $3, $4)
      RETURNING global_seq, event_id;
    `,
    ["TransactionPosted", 1, eventPayload, input.idempotencyKey]
  )
  if (!events[0]) throw new Error("event insert returned no row")

  const { global_seq: globalSeq, event_id: eventId } = events[0]

  // 5. for each posting: upsert balances (amount += p.amount, last_event_seq = global_seq)
  for (const leg of input.postings) {
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
