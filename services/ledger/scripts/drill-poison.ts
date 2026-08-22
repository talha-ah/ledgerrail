/**
 * Stage 2.5 drill - head-of-line blocking.
 *
 * The suite proves the handler returns normally for a poison message. That is the
 * precondition, not the property: what actually unblocks the partition is kafkajs
 * committing the offset when eachMessage resolves, and no unit test runs kafkajs.
 * So, this produces a poison message and a good one behind it, runs the real
 * consumer, and then asks the broker where the group's cursor ended up.
 *
 * Run with the stack up: docker compose -f infra/docker-compose.yml up -d
 */

import type { PoolClient } from "pg"

import { createAccounts } from "../test/utils.js"
import { getPool, transaction } from "../src/db.js"
import { bootstrap, shutdown } from "../src/index.js"
import { randomString } from "../src/utils/helpers.js"
import type { Event, EventVersion } from "../src/types.js"
import type { EventEnvelope } from "../src/kafka/index.js"
import { withDeadLetter } from "../src/deadLetters/withDeadLetter.js"
import { makeMessageHandler } from "../src/projections/balanceProjection/consumer.js"
import {
  postTransaction,
  type PostTransactionInput,
} from "../src/commands/postTransaction/postTransaction.js"
import {
  getKafka,
  createTopic,
  transactionKey,
  createConsumer,
  type ConsumerHandle,
  type RawMessageHandler,
} from "../src/kafka/index.js"

const AMOUNT = 100n
const POISON = "{ not json"

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(`[DRILL] FAILED - ${message}`)

  console.log(`[DRILL] ok - ${message}`)
}

function deferred<T>() {
  let resolve!: (v: T) => void
  const promise = new Promise((r) => (resolve = r))

  return { promise, resolve }
}

async function postEvent(
  systemId: number,
  assetId: number,
  client: PoolClient
) {
  const input: PostTransactionInput = {
    idempotencyKey: randomString(),
    currency: "PKR",
    postings: [
      { accountId: systemId, amount: -AMOUNT },
      { accountId: assetId, amount: AMOUNT },
    ],
  }

  const { eventId } = await postTransaction(input, client)

  const { rows } = await client.query<Event>(
    `SELECT global_seq, event_id, event_type, event_version, payload, created_at
    FROM events WHERE event_id = $1`,
    [eventId]
  )

  const event = rows[0]
  if (!event) throw new Error("event vanished after posting")

  const envelope: EventEnvelope = {
    payload: event.payload,
    event_id: event.event_id,
    event_type: event.event_type,
    global_seq: String(event.global_seq),
    created_at: event.created_at.toISOString(),
    event_version: event.event_version as EventVersion,
  }

  return envelope
}

async function main() {
  await bootstrap()

  const pool = getPool()
  const kafka = getKafka()

  // A throwaway topic with exactly ONE partition. Head-of-line blocking is a
  // per-partition property, so two messages spread across partitions would prove
  // nothing — one partition removes the question. Fresh per run so offsets start
  // at 0 and the group has no cursor to inherit.
  const topic = `ledger.events.drill.${Date.now()}`
  const group = `drill-poison-${randomString()}`

  const admin = kafka.admin()
  const producer = kafka.producer()

  let consumer: ConsumerHandle | null = null

  try {
    await createTopic(topic, 1, 1)
    await admin.connect()
    await producer.connect()

    const { envelope, assetId } = await transaction(async (client) => {
      const { s, a } = await createAccounts(client)
      const envelope = await postEvent(s, a, client)
      return { envelope, assetId: a }
    })

    // Order is the whole point: the unreadable one first, the good one behind it.
    // Note this bypasses ProducerHandle — its signature takes an EventEnvelope,
    // and the drill needs to send something that deliberately isn't one.
    await producer.send({
      topic,
      messages: [
        { key: transactionKey("poison"), value: Buffer.from(POISON, "utf-8") },
        {
          key: transactionKey(envelope.event_id),
          value: JSON.stringify(envelope),
        },
      ],
    })
    console.log(`[DRILL] produced 2 messages to ${topic} (poison at offset 0)`)

    const handled = deferred<void>()
    let count = 0

    const dlq = withDeadLetter(makeMessageHandler(transaction, group), {
      pool,
      consumerGroup: group,
    })

    const onMessage: RawMessageHandler = async (msg) => {
      await dlq(msg)
      if (++count === 2) handled.resolve()
    }

    consumer = await createConsumer({
      topic,
      onMessage,
      groupId: group,
      fromBeginning: true, // only meaningful because the group is new
    })

    let timer: NodeJS.Timeout | undefined
    const timedOut = new Promise<"timeout">((resolve) => {
      timer = setTimeout(() => resolve("timeout"), 30_000)
    })

    const outcome = await Promise.race([
      handled.promise.then(() => "handled" as const),
      timedOut,
    ])
    clearTimeout(timer)

    assert(outcome === "handled", `consumer saw both messages (saw ${count})`)

    // Must come before fetchOffsets: kafkajs commits pending offsets when the
    // runner stops. Reading the cursor of a consumer that is still running is a
    // race, and one that would usually pass.
    await consumer.disconnect()
    consumer = null

    const [committed] = await admin.fetchOffsets({
      groupId: group,
      topics: [topic],
    })
    const offset = Number(committed?.partitions[0]?.offset ?? -1)

    // THE assertion. 2 means the cursor is past both messages — including the
    // poison one at offset 0. Anything less and the partition is still stuck on it.
    assert(
      offset === 2,
      `committed offset is past the poison message (got ${offset})`
    )

    const { rows: dead } = await pool.query<{
      kafka_offset: BigInt
      raw: Buffer | null
      error: string
    }>(
      `SELECT kafka_offset, raw, error FROM dead_letters
      WHERE consumer_group = $1 AND topic = $2`,
      [group, topic]
    )

    assert(
      dead.length === 1,
      `exactly one dead letter recorded (got ${dead.length})`
    )
    assert(
      Number(dead[0]?.kafka_offset) === 0,
      `the dead letter is the message at offset 0`
    )
    assert(
      dead[0]?.raw?.toString("utf-8") === POISON,
      "the raw bytes were preserved"
    )

    const { rows: balances } = await pool.query<{ amount: bigint }>(
      `SELECT amount FROM balances WHERE account_id = $1`,
      [assetId]
    )

    assert(
      balances[0]?.amount === AMOUNT,
      `the message behind it was applied (balance ${balances[0]?.amount ?? 0n})`
    )

    const { rows: inbox } = await pool.query<{ event_id: string }>(
      `SELECT event_id FROM inbox WHERE consumer_group = $1`,
      [group]
    )

    assert(
      inbox.length === 1,
      `only the good event was claimed (${inbox.length} inbox rows)`
    )
    console.log(
      "\n[DRILL] passed — a poison message did not block the partition"
    )
  } finally {
    await consumer?.disconnect()
    await producer?.disconnect()
    // Delete the throwaway topic. Safe because this run created it; never do this
    // to a topic anything else consumes.
    await admin.deleteTopics({ topics: [topic] }).catch(() => {})
    await admin.disconnect()
    await shutdown()
  }
}

if (import.meta.main) {
  main().catch((err) => {
    console.error(err)
    process.exit(1)
  })
}
