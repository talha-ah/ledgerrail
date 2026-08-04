/**
 * This will be triggered by an external cron scheduler instead of
 * running it continuously inside the app
 */
import type { Pool } from "pg"

import { getPool } from "../db.js"
import { bootstrap } from "../index.js"
import { TOPICS } from "../kafka/topics.js"
import type { Event, Outbox } from "../types.js"
import type { ProducerHandle } from "../kafka/index.js"
import { createTopic, createProducer } from "../kafka/index.js"

const BATCH_SIZE = 10

export async function publishBatch(
  pool: Pool,
  producer: ProducerHandle,
  limit = BATCH_SIZE
): Promise<number> {
  const { rows: outbox } = await pool.query<
    Pick<Outbox, "event_id" | "topic" | "partition_key"> &
      Pick<Event, "event_type" | "event_version" | "payload">
  >(
    `
      SELECT o.event_id, o.topic, o.partition_key, e.event_type, e.event_version, e.payload
      FROM outbox as o
      JOIN events e on o.event_id = e.event_id
      WHERE topic = $1 AND published_at is NULL
      ORDER BY o.global_seq
      LIMIT $2;
    `,
    [TOPICS.LEDGER_EVENTS, limit]
  )

  if (outbox.length === 0) return 0

  // Kafka
  for (const o of outbox) {
    await producer.send(o.topic, o.partition_key, {
      payload: o.payload,
      event_id: o.event_id,
      event_type: o.event_type,
      event_version: o.event_version,
    })

    await pool.query(
      `
        UPDATE outbox
        SET published_at = NOW()
        WHERE event_id = $1
      `,
      [o.event_id]
    )
  }

  return outbox.length
}

export async function main() {
  await bootstrap()

  await createTopic(TOPICS.LEDGER_EVENTS)
  const producer = await createProducer()

  let n: number

  do {
    n = await publishBatch(getPool(), producer)
  } while (n > 0)

  await producer.disconnect()
}
