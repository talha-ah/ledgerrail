/**
 * Stage 2 drill — the relay dies between produce and mark.
 *
 * ADR 0003 chose produce-then-mark, which makes delivery at-least-once: a crash
 * in that gap republishes the message. This is the drill that shows the duplicate
 * actually happening, and the inbox absorbing it.
 *
 * Run with the consumer STOPPED. balances has exactly one writer (ADR 0004) and
 * for the length of this drill that writer is the drill.
 */
import type { Pool } from "pg"
import { fileURLToPath } from "node:url"
import { spawn } from "node:child_process"

import { createAccounts } from "../../test/utils.js"
import { getPool, transaction } from "../../src/db.js"
import { publishBatch } from "../../src/relay/relay.js"
import { bootstrap, shutdown } from "../../src/index.js"
import { randomString } from "../../src/utils/helpers.js"
import { withDeadLetter } from "../../src/deadLetters/withDeadLetter.js"
import { postTransaction } from "../../src/commands/postTransaction/postTransaction.js"
import { makeMessageHandler } from "../../src/projections/balanceProjection/consumer.js"
import {
  TOPICS,
  createProducer,
  createConsumer,
  transactionKey,
  type ProducerHandle,
  type RawMessageHandler,
  type ConsumerHandle,
} from "../../src/kafka/index.js"

const AMOUNT = 100n
const KILL_CODE = 137

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(`[DRILL] FAILED - ${message}`)
  console.log(`[DRILL] ok - ${message}`)
}

function deferred<T>() {
  let resolve!: (a: T) => void
  const promise = new Promise((r) => (resolve = r))
  return { promise, resolve }
}

function killAfterSend(producer: ProducerHandle): ProducerHandle {
  return {
    async send(topic, key, value) {
      await producer.send(topic, key, value)
      console.log(`💥 published ${value.event_id}, killing before the mark`)
      process.exit(KILL_CODE)
    },
    disconnect: () => producer.disconnect(),
  }
}

async function drainOutbox(pool: Pool, producer: ProducerHandle) {
  let n: number
  do {
    n = await publishBatch(pool, producer)
  } while (n > 0)
}

async function unpublished(pool: Pool, eventId: string): Promise<boolean> {
  const { rows } = await pool.query<{ published_at: Date | null }>(
    `SELECT published_at FROM outbox WHERE event_id=$1`,
    [eventId]
  )
  if (!rows[0]) throw new Error("no outbox row for the drill's event")
  return rows[0].published_at === null
}

async function child() {
  await bootstrap()

  const producer = await createProducer()
  // limit 1: the parent dranined the outbox first, so the only row left is ours.
  await publishBatch(getPool(), killAfterSend(producer), 1)

  throw new Error("unreachable - killAfterSend should have killed the process")
}

async function parent() {
  await bootstrap()

  const pool = getPool()
  const producer = await createProducer()
  const group = `drill-relay-${randomString()}`

  let consumer: ConsumerHandle | null = null

  try {
    // Anything already pending would be published ahead of ours, so the child
    // would kill on somebody else's message.
    await drainOutbox(pool, producer)

    const { s, a } = await transaction((client) => createAccounts(client))
    const assetId = a
    const systemId = s

    let key = ""
    let copies = 0
    // Started before the event is produced, with a group that has no cursor, so
    // it sees the drill's messages and nothing that came before them.
    const seen = deferred<void>()

    const dlq = withDeadLetter(makeMessageHandler(transaction, group), {
      pool,
      consumerGroup: group,
    })

    const onMessage: RawMessageHandler = async (msg) => {
      // Skipping is normally the cardinal sin — it commits the offset and loses
      // the message. Here the group exists for this drill alone and its cursor is
      // discarded afterwards, so every message that isn't ours belongs to some
      // other run and must not be re-applied to the balances it already moved.
      if (msg.key !== key) return

      await dlq(msg)
      if (++copies === 2) seen.resolve()
    }

    consumer = await createConsumer({
      onMessage,
      groupId: group,
      fromBeginning: true,
      topic: TOPICS.LEDGER_EVENTS,
    })

    const { eventId } = await transaction((client) =>
      postTransaction(
        {
          currency: "PKR",
          idempotencyKey: randomString(),
          postings: [
            { accountId: systemId, amount: -AMOUNT },
            { accountId: assetId, amount: AMOUNT },
          ],
        },
        client
      )
    )
    key = transactionKey(eventId)

    console.log(`[DRILL] spawning a relay that will die mid-publish`)

    const proc = spawn("tsx", [fileURLToPath(import.meta.url), "--child"], {
      stdio: "inherit",
    })
    const code = await new Promise<number>((r) =>
      proc.on("exit", (c) => r(c ?? -1))
    )

    // Not "non-zero": the exact code we chose. A child that dies on a module it
    // cannot resolve also exits non-zero, and would look like a passing drill.
    assert(code === KILL_CODE, `relay died mid-publish (exit ${code})`)
    assert(
      await unpublished(pool, eventId),
      "the outbox row is still unpublished — produced, not marked"
    )

    // The restart. The relay has no memory of the first attempt, so it publishes
    // a second copy of a message the broker already has.
    console.log("[DRILL] restarting the relay")
    await drainOutbox(pool, producer)
    assert(
      !(await unpublished(pool, eventId)),
      "the outbox row is marked published after the restart"
    )

    let timer: NodeJS.Timeout | undefined
    const timedOut = new Promise<"timeout">((resolve) => {
      timer = setTimeout(() => resolve("timeout"), 30_000)
    })
    const outcome = await Promise.race([
      seen.promise.then(() => "seen" as const),
      timedOut,
    ])
    clearTimeout(timer)
    assert(outcome === "seen", `both copies were delivered (saw ${copies})`)

    const { rows: balances } = await pool.query<{ amount: bigint }>(
      `SELECT amount FROM balances WHERE account_id = $1`,
      [assetId]
    )

    // The point of the whole drill. Two deliveries, one movement of money.
    assert(
      balances[0]?.amount === AMOUNT,
      `money moved exactly once (balance ${balances[0]?.amount ?? 0n}) - if this is ${AMOUNT + 2n}n, the balances was running too`
    )

    const { rows: inbox } = await pool.query(
      "SELECT event_id FROM inbox WHERE consumer_group = $1 AND event_id = $2",
      [group, eventId]
    )
    assert(inbox.length === 1, "the inbox claimed the event once")

    console.log(
      "\n[DRILL] passed — at-least-once delivery, exactly-once effect"
    )
  } finally {
    await consumer?.disconnect()
    await producer.disconnect()
    await shutdown()
  }
}

const run = process.argv.includes("--child") ? child : parent

run().catch((err) => {
  console.error(err)
  process.exit(1)
})
