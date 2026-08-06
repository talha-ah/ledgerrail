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
  runInTransaction: RunInTransaction
): MessageHandler {
  return async ({ topic, partition, offset, key, value }: IncomingMessage) => {
    if (!validateEvent(value)) {
      console.error(
        `[CONSUMER] Invalid value detected for: ${topic}:${partition}:${offset}:${key}:${describeValue(value)}`
      )
      return
    }

    await runInTransaction((c) => applyToBalances(value, c))

    console.log(
      `[CONSUMER] applied ${value.event_id} (${topic}@${partition}@${offset})`
    )
  }
}

export async function main() {
  await bootstrap()

  const consumer = await createConsumer({
    topic: TOPICS.LEDGER_EVENTS,
    groupId: GROUPS.BALANCES_PROJECTION,
    onMessage: makeMessageHandler(transaction),
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
