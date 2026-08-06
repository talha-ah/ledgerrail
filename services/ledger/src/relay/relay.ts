/**
 * This will be triggered by an external cron scheduler instead of
 * running it continuously inside the app
 */
import type { Pool } from "pg"

import { getPool } from "../db.js"
import type { Event, Outbox } from "../types.js"
import { bootstrap, shutdown } from "../index.js"
import { TOPICS, createProducer, type ProducerHandle } from "../kafka/index.js"

const BATCH_SIZE = 10

export async function publishBatch(
  pool: Pool,
  producer: ProducerHandle,
  limit = BATCH_SIZE
): Promise<number> {
  const { rows: outbox } = await pool.query<
    Pick<Outbox, "global_seq" | "event_id" | "topic" | "partition_key"> &
      Pick<Event, "event_type" | "event_version" | "payload" | "created_at">
  >(
    `
      SELECT o.global_seq, o.event_id, o.topic, o.partition_key, e.event_type, e.event_version, e.payload, e.created_at
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
      global_seq: String(o.global_seq),
      created_at: o.created_at.toISOString(),
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
  const producer = await createProducer()

  try {
    let total = 0
    let n: number

    do {
      n = await publishBatch(getPool(), producer)
      if (n > 0) console.log(`[RELAY] published ${n} event(s)`)
      total += n
    } while (n > 0)

    console.log(`[RELAY] done — ${total} event(s) published`)
  } finally {
    await producer.disconnect()
    await shutdown()
  }
}

// only auto-start if I'm the program, not if I'm a library someone imported
if (import.meta.main) {
  main().catch((err) => {
    console.error(err)
    process.exit(1)
  })
}
