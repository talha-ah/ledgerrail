import type { PoolClient } from "pg"

import type { Event, Balance, BalanceSnapshot } from "../../types.js"

async function assertInvariant(client: PoolClient) {
  const { rows: balances } = await client.query<Balance>(
    "SELECT * FROM balances"
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
  const { rows: balanceSnapshots } = await client.query<BalanceSnapshot>(
    `
    SELECT * FROM balance_snapshots
    WHERE as_of_event_seq = (SELECT MAX(as_of_event_seq) FROM balance_snapshots)
    ORDER BY as_of_event_seq ASC
    `
  )

  // Fund balance using last snapshot
  for (const snapshot of balanceSnapshots) {
    await client.query(
      `
        INSERT INTO balances (account_id, amount, last_event_seq, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $4); -- Loud error if any conflict
      `,
      [
        snapshot.account_id,
        BigInt(snapshot.amount),
        snapshot.as_of_event_seq,
        snapshot.created_at,
      ]
    )
  }

  // Reads all events in global_seq order
  const { rows: events } = await client.query<
    Pick<Event, "global_seq" | "payload" | "created_at">
  >(
    `
    SELECT global_seq, payload, created_at
    FROM events
    WHERE global_seq > $1
    ORDER BY global_seq ASC
    `,
    [balanceSnapshots[0]?.as_of_event_seq || "0"]
  )

  // Replay each event
  for (const event of events) {
    await applyToBalances(event, client)
  }

  // Asset invariant to be zero
  await assertInvariant(client)
}

export async function balancesSnapshot(client: PoolClient) {
  // Fetch latest event seq to use as a snapshot point
  const { rows: latestEvent } = await client.query<{ seq: number }>(
    "SELECT MAX(global_seq) AS seq FROM events"
  )
  if (!latestEvent[0]?.seq) throw new Error("Events empty")

  const { rows: balances } = await client.query<Balance>(
    "SELECT * FROM balances"
  )

  for (const balance of balances) {
    await client.query(
      `
      INSERT INTO balance_snapshots (account_id, amount, as_of_event_seq)
      VALUES ($1, $2, $3)
      ON CONFLICT (account_id, as_of_event_seq) DO NOTHING;
      `,
      [balance.account_id, balance.amount, latestEvent[0].seq]
    )
  }
}
