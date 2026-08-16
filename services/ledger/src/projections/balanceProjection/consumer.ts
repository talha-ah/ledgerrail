import type { PoolClient } from "pg"

import { transaction } from "../../db.js"
import { bootstrap, shutdown } from "../../index.js"
import { applyToBalances } from "./balanceProjection.js"
import { validateEvent } from "../../utils/validations.js"
import {
  TOPICS,
  GROUPS,
  describeValue,
  createConsumer,
  type IncomingMessage,
  type MessageHandler,
} from "../../kafka/index.js"

type RunInTransaction = <T>(cb: (c: PoolClient) => Promise<T>) => Promise<T>

export function makeMessageHandler(
  runInTransaction: RunInTransaction,
  consumerGroup: string
): MessageHandler {
  return async ({ topic, partition, offset, key, value }: IncomingMessage) => {
    if (!validateEvent(value)) {
      console.error(
        `[CONSUMER] rejected: ${topic}:${partition}:${offset}:${key}:${describeValue(value)}`
      )
      return
    }

    const applied = await runInTransaction(async (client) => {
      // Claim the event for this group. The primary key is what makes this safe;
      // DO NOTHING turns a duplicate into zero rows rather than aborting the
      // transaction — the same trick postTransaction uses for idempotency keys.
      const { rows } = await client.query(
        `
        INSERT INTO inbox (consumer_group, event_id, partition, kafka_offset)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (consumer_group, event_id) DO NOTHING
        RETURNING event_id;
        `,
        [consumerGroup, value.event_id, partition, offset]
      )

      if (rows.length === 0) return false // this group already applied it

      await applyToBalances(value, client)
      return true
    })

    console.log(
      applied
        ? `[CONSUMER] applied ${value.event_id} (${topic}/${partition}@${offset})`
        : `[CONSUMER] skipped duplicate ${value.event_id} (${topic}/${partition}@${offset})`
    )
  }
}

export async function main() {
  await bootstrap()

  const consumer = await createConsumer({
    topic: TOPICS.LEDGER_EVENTS,
    groupId: GROUPS.BALANCES_PROJECTION,
    onMessage: makeMessageHandler(transaction, GROUPS.BALANCES_PROJECTION),
  })

  let stopping = false

  const stop = async () => {
    if (stopping) return
    stopping = true

    console.log("[CONSUMER] shutting down")
    await consumer.disconnect()
    await shutdown()
    process.exit(0)
  }

  process.once("SIGINT", stop)
  process.once("SIGTERM", stop)
}

if (import.meta.main) {
  main().catch((err) => {
    console.error(err)
    process.exit(1)
  })
}
