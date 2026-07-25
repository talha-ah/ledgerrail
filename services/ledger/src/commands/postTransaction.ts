import type { PoolClient } from "pg"

import type { Currency } from "@ledgerrail/money"

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
  const { rows: accounts } = await client.query<{
    id: number
    currency: Currency
  }>(`SELECT id, currency FROM accounts WHERE id = ANY($1)`, [
    input.postings.map((p) => p.accountId),
  ])

  const accountsSet = new Set(accounts.map((a) => a.id))

  if (input.postings.some((p) => !accountsSet.has(p.accountId))) {
    throw new Error("Invalid Accounts")
  }
  if (accounts.some((a) => a.currency !== input.currency)) {
    throw new Error("Invalid Currency")
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
  const { rows: events } = await client.query<{
    global_seq: string
    event_id: string
  }>(
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
  for (const posting of input.postings) {
    await client.query(
      `
        INSERT INTO balances (account_id, amount, last_event_seq)
        VALUES ($1, $2, $3)
        ON CONFLICT (account_id)
        DO UPDATE SET
          amount = balances.amount + EXCLUDED.amount, -- EXCLUDED is the INSERT row conflicted
          last_event_seq = EXCLUDED.last_event_seq;
      `,
      [posting.accountId, posting.amount, globalSeq]
    )
  }

  // 6. return { globalSeq, eventId }
  return { globalSeq, eventId }
}
