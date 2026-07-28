import type { PoolClient } from "pg"

import type { Event, Balance } from "../../types.js"

async function assertInvariant(client: PoolClient) {
  const { rows: balances } = await client.query<Balance>(
    "SELECT account_id, amount FROM balances"
  )

  const totalAmount = balances.reduce((a, b) => a + BigInt(b.amount), 0n)
  if (totalAmount !== 0n) throw new Error("Events replay failed")
}

export async function applyToBalances(
  event: Pick<Event, "global_seq" | "created_at" | "payload">,
  client: PoolClient
) {
  const payload = event.payload

  // Recomputes balances from scratch
  for (const leg of payload.postings) {
    await client.query(
      `
        INSERT INTO balances (account_id, amount, last_event_seq, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $4)
        ON CONFLICT (account_id)
        DO UPDATE SET
          amount = balances.amount + EXCLUDED.amount, -- EXCLUDED is the INSERT row conflicted
          last_event_seq = EXCLUDED.last_event_seq,
          updated_at = EXCLUDED.updated_at;
      `,
      [leg.accountId, BigInt(leg.amount), event.global_seq, event.created_at]
    )
  }
}

export async function rebuildBalances(client: PoolClient) {
  // Deletes all rows of balances
  await client.query("DELETE FROM balances")

  // Reads all events in global_seq order
  const { rows: events } = await client.query<
    Pick<Event, "global_seq" | "payload" | "created_at">
  >(
    "SELECT global_seq, payload, created_at FROM events ORDER BY global_seq ASC"
  )

  // Replay each event
  for (const event of events) {
    await applyToBalances(event, client)
  }

  await assertInvariant(client)
}
