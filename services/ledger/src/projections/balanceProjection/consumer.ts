import type { PoolClient } from "pg"

import { getPool, transaction } from "../../db.js"
import { bootstrap, shutdown } from "../../index.js"
import { PermanentError } from "../../utils/error.js"
import { applyToBalances } from "./balanceProjection.js"
import { validateEvent } from "../../utils/validations.js"
import { withDeadLetter } from "../../deadLetters/withDeadLetter.js"
import {
  TOPICS,
  GROUPS,
  describeValue,
  createConsumer,
  type DecodedMessage,
  type MessageHandler,
} from "../../kafka/index.js"

type RunInTransaction = <T>(cb: (c: PoolClient) => Promise<T>) => Promise<T>

export function makeMessageHandler(
  runInTransaction: RunInTransaction,
  consumerGroup: string
): MessageHandler {
  return async ({ topic, partition, offset, value }: DecodedMessage) => {
    // Throwing rather than returning: a message this consumer cannot read is a
    // permanent failure, and it is the caller's business what to do with it.
    // Returning quietly would commit the offset and lose the message.
    if (!validateEvent(value)) {
      throw new PermanentError(`unrecognized message: ${describeValue(value)}`)
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

  const group = GROUPS.BALANCES_PROJECTION

  const consumer = await createConsumer({
    groupId: group,
    topic: TOPICS.LEDGER_EVENTS,
    onMessage: withDeadLetter(makeMessageHandler(transaction, group), {
      pool: getPool(),
      consumerGroup: group,
    }),
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
