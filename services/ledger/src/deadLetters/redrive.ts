/**
 * Run after fixing whatever made the messages unprocessable. Like the relay,
 * this is a one-shot batch a human or a scheduler triggers — not a loop inside
 * the consumer, because "try the broken thing again" is an operational decision.
 */
import type { Pool } from "pg"

import { decode } from "./withDeadLetter.js"
import { getPool, transaction } from "../db.js"
import { bootstrap, shutdown } from "../index.js"
import { errorMessage, isPermanent } from "../utils/error.js"
import { GROUPS, type MessageHandler } from "../kafka/index.js"
import { listUnresolved, markResolved } from "./deadLetters.js"
import { makeMessageHandler } from "../projections/balanceProjection/consumer.js"

export interface RedriveResult {
  attempted: number
  resolved: number
  failed: number
}

export async function redrive(
  pool: Pool,
  consumerGroup: string,
  inner: MessageHandler,
  limit = 50
): Promise<RedriveResult> {
  const rows = await listUnresolved(pool, consumerGroup, limit)

  let failed = 0
  let resolved = 0

  for (const row of rows) {
    const where = `${row.topic}/${row.partition}@${row.kafka_offset}`

    try {
      // The same handler the consumer runs, so the same inbox claim applies:
      // If the consumer somehow already applied this event, the redrive is a
      // no-op rather than a second author.
      await inner({
        topic: row.topic,
        partition: row.partition,
        offset: String(row.kafka_offset),
        key: row.message_key,
        raw: row.raw,
        value: decode(row.raw),
      })

      await markResolved(pool, row)
      resolved++
      console.log(`[DLQ] redrove ${where}`)
    } catch (err) {
      // A transient failure aborts the batch rather than being recorded as a
      // verdict on this row. The rows share one pool, so if the database has
      // gone away, every remaining row would "fail" identically and the summary
      // would report fifty broken messages instead of one broken dependency.
      // Rerunning after it recovers costs nothing — unresolved rows are still there.
      if (!isPermanent(err)) throw err

      // Left unresolved on purpose: a redrive that permanently failed has
      // established that the fix is not in yet, and the row is the record of that.
      failed++
      console.error(`[DLQ] still failing ${where}:${errorMessage(err)}`)
    }
  }

  return { attempted: rows.length, resolved, failed }
}

export async function main() {
  await bootstrap()

  try {
    const group = GROUPS.BALANCES_PROJECTION
    const { attempted, resolved, failed } = await redrive(
      getPool(),
      group,
      makeMessageHandler(transaction, group)
    )

    console.log(
      `[DLQ] done — ${attempted} attempted, ${resolved} resolved, ${failed} still failing`
    )
  } finally {
    await shutdown()
  }
}

if (import.meta.main) {
  main().catch((err) => {
    console.error(err)
    process.exit(1)
  })
}
