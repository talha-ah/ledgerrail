import type { PoolClient } from "pg"

import { upcast } from "../../events/upcast.js"
import type { EventEnvelope } from "../../kafka/index.js"
import type { Event, Balance, BalanceSnapshot } from "../../types.js"

async function assertInvariant(client: PoolClient) {
  const { rows: balances } = await client.query<Balance>(
    "SELECT * FROM balances"
  )

  const totalAmount = balances.reduce((a, b) => a + BigInt(b.amount), 0n)
  if (totalAmount !== 0n) throw new Error("Events replay failed")
}

export async function applyToBalances(
  event: Pick<
    EventEnvelope,
    "event_type" | "event_version" | "global_seq" | "created_at" | "payload"
  >,
  client: PoolClient
) {
  // Here rather than at the call sites. There are three callers — the consumer,
  // the rebuild and the redrive — and the rebuild is the one that would be
  // forgotten, because nothing in production exercises it.
  const { payload } = upcast(event)

  // Recomputes balances from scratch
  for (const leg of payload.postings) {
    await client.query(
      `
        INSERT INTO balances (account_id, amount, last_event_seq, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $4)
        ON CONFLICT (account_id)
        DO UPDATE SET
          amount = balances.amount + EXCLUDED.amount,
          -- EXCLUDED is the INSERT row conflicted
          -- GREATEST, not assignment: a redriven dead letter applies an event
          -- older than ones already here. The amounts survive that (adding
          -- deltas is commutative) but the cursor must not walk backwards, or it
          -- claims the projection is behind ground it has already covered.
          last_event_seq = GREATEST(balances.last_event_seq, EXCLUDED.last_event_seq),
          updated_at = GREATEST(balances.updated_at, EXCLUDED.updated_at);
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
    Pick<
      Event,
      "event_type" | "event_version" | "global_seq" | "payload" | "created_at"
    >
  >(
    `
    SELECT event_type, event_version, global_seq, payload, created_at
    FROM events
    WHERE global_seq > $1
    ORDER BY global_seq ASC
    `,
    [balanceSnapshots[0]?.as_of_event_seq || "0"]
  )

  // Replay each event
  for (const event of events) {
    await applyToBalances(
      {
        payload: event.payload,
        event_type: event.event_type,
        event_version: event.event_version,
        global_seq: String(event.global_seq),
        created_at: event.created_at.toISOString(),
      },
      client
    )
  }

  // Asset invariant to be zero
  await assertInvariant(client)
}

export async function balancesSnapshot(client: PoolClient) {
  // The projection's own watermark, not the journal's head. The amounts below
  // come from `balances`, so the cursor must describe `balances` too — stamping
  // the latest event id claims the projection had caught up when it may not
  // have, and a later rebuild then replays only events after a point the
  // projection never reached. The lost history is invisible: Σ = 0 holds,
  // because whatever survives nets to zero on its own.
  const { rows: applied } = await client.query<{ seq: bigint | null }>(
    "SELECT MAX(last_event_seq) AS seq FROM balances"
  )

  const asOf = applied[0]?.seq
  if (asOf === null || asOf === undefined) {
    throw new Error("refusing to snapshot an empty projection")
  }

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
      [balance.account_id, balance.amount, String(asOf)]
    )
  }
}
