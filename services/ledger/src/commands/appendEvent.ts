import type { PoolClient } from "pg"

import { TOPICS, transactionKey } from "../kafka/index.js"
import type { Event, EventType, Posting } from "../types.js"

export interface AppendResult {
  eventId: string
  globalSeq: string
  replayed: boolean
}

export async function findByIdempotencyKey(
  idempotencyKey: string,
  client: PoolClient
): Promise<AppendResult | null> {
  const { rows } = await client.query<Pick<Event, "event_id" | "global_seq">>(
    "SELECT event_id, global_seq FROM events WHERE idempotency_key = $1",
    [idempotencyKey]
  )

  const row = rows[0]
  if (!row) return null

  return {
    replayed: true,
    eventId: row.event_id,
    globalSeq: String(row.global_seq),
  }
}

export async function appendEvent(
  eventType: EventType,
  eventVersion: number,
  payload: unknown,
  idempotencyKey: string,
  client: PoolClient
): Promise<AppendResult> {
  const { rows } = await client.query<Pick<Event, "event_id" | "global_seq">>(
    `INSERT INTO events (event_type, event_version, payload, idempotency_key)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (idempotency_key) DO NOTHING
    RETURNING event_id, global_seq;
  `,
    [eventType, eventVersion, payload, idempotencyKey]
  )

  const row = rows[0]

  if (!row) {
    // A replay. Catching 23505 is impossible inside a transaction — the first
    // failed statement aborts it — so DO NOTHING plus a second SELECT is the
    // only route (1.4).
    const { rows: original } = await client.query<
      Pick<Event, "event_id" | "global_seq">
    >("SELECT event_id, global_seq FROM events WHERE idempotency_key = $1", [
      idempotencyKey,
    ])

    if (!original[0]) {
      throw new Error(
        "A replay was detected but the original isn't visible yet"
      )
    }

    return {
      replayed: true,
      eventId: original[0].event_id,
      globalSeq: String(original[0].global_seq),
    }
  }

  await client.query(
    `INSERT INTO outbox (event_id, global_seq, topic, partition_key)
    VALUES ($1, $2, $3, $4);`,
    [
      row.event_id,
      row.global_seq,
      TOPICS.LEDGER_EVENTS,
      transactionKey(row.event_id),
    ]
  )

  return {
    replayed: false,
    eventId: row.event_id,
    globalSeq: String(row.global_seq),
  }
}

export async function applyToAccountBalances(
  postings: Posting[],
  client: PoolClient
) {
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
      [leg.accountId, BigInt(leg.amount)]
    )
  }
}
