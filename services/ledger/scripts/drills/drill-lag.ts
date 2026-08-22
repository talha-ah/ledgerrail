/**
 * Stage 2 drill — consumer lag.
 *
 * lag = high water mark − committed offset. A consumer group that isn't reading
 * accumulates it and loses nothing; starting it drains the backlog. Run with the
 * balances consumer STOPPED — balances has exactly one writer (ADR 0004), and for
 * the length of this drill that writer is the drill.
 */
import type { Pool } from "pg"
import type { Admin } from "kafkajs"

import { getPool, transaction } from "../../src/db.js"
import { publishBatch } from "../../src/relay/relay.js"
import { bootstrap, shutdown } from "../../src/index.js"
import { randomString } from "../../src/utils/helpers.js"
import { withDeadLetter } from "../../src/deadLetters/index.js"
import { postTransaction } from "../../src/commands/postTransaction/index.js"
import { makeMessageHandler } from "../../src/projections/balanceProjection/consumer.js"
import {
  getKafka,
  TOPICS,
  createProducer,
  createConsumer,
  type ConsumerHandle,
  type ProducerHandle,
  type RawMessageHandler,
} from "../../src/kafka/index.js"

const AMOUNT = 100n
const BACKLOG = 5

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(`[DRILL] FAILED — ${message}`)
  console.log(`[DRILL] ok — ${message}`)
}

/**
 * Summed across partitions, because lag is per partition and this topic has
 * three of them — reading partition 0 would report a third of the truth.
 */
async function lagFor(admin: Admin, groupId: string, topic: string) {
  const [ends, committed] = await Promise.all([
    admin.fetchTopicOffsets(topic),
    admin.fetchOffsets({ groupId, topics: [topic] }),
  ])

  const highWaterMark = new Map(
    ends.map((e) => [e.partition, Number(e.offset)])
  )

  const partitions = committed[0]?.partitions ?? []

  // A sum over an empty list is zero, so a group that does not exist would
  // otherwise report itself perfectly caught up — and the assertion meant to
  // establish that precondition would pass precisely when it is absent.
  if (partitions.length !== ends.length) {
    throw new Error(
      `group ${groupId} has cursors for ${partitions.length} of ${ends.length} partitions — lag is undefined`
    )
  }

  let lag = 0
  for (const p of partitions) {
    const cursor = Number(p.offset)

    // -1 means the group has never committed here. Subtracting it would report
    // a lag one larger than the partition, which reads like an off-by-one bug
    // in your own code for about half an hour.
    if (cursor < 0) {
      throw new Error(
        `partition ${p.partition} has no committed offset — lag is undefined, not ${highWaterMark.get(p.partition)}`
      )
    }

    lag += (highWaterMark.get(p.partition) ?? 0) - cursor
  }

  return lag
}

async function waitFor(
  predicate: () => Promise<boolean>,
  what: string,
  ms = 30_000
) {
  const deadline = Date.now() + ms
  while (Date.now() < deadline) {
    if (await predicate()) return
    await new Promise((r) => setTimeout(r, 250))
  }
  throw new Error(`[DRILL] FAILED — timed out waiting for ${what}`)
}

async function createAccount(type: "system" | "asset", pool: Pool) {
  const { rows } = await pool.query<{ id: number }>(
    `INSERT INTO accounts (account_number, currency, type)
     VALUES ($1, 'PKR', $2) RETURNING id;`,
    [randomString(), type]
  )
  if (!rows[0]) throw new Error(`could not create ${type} account`)
  return rows[0].id
}

async function drainOutbox(pool: Pool, producer: ProducerHandle) {
  let n: number
  do {
    n = await publishBatch(pool, producer)
  } while (n > 0)
}

async function postAndRelay(
  pool: Pool,
  producer: ProducerHandle,
  systemId: number,
  assetId: number
) {
  await transaction((client) =>
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

  await drainOutbox(pool, producer)
}

async function main() {
  await bootstrap()

  const pool = getPool()
  const admin = getKafka().admin()
  const topic = TOPICS.LEDGER_EVENTS
  const producer = await createProducer()
  const group = `drill-lag-${randomString()}`

  let consumer: ConsumerHandle | null = null

  const start = async () => {
    const dlq = withDeadLetter(makeMessageHandler(transaction, group), {
      pool,
      consumerGroup: group,
    })
    const onMessage: RawMessageHandler = async (msg) => dlq(msg)
    return createConsumer({ topic, groupId: group, onMessage })
  }

  const balanceOf = async (accountId: number) => {
    const { rows } = await pool.query<{ amount: bigint }>(
      "SELECT amount FROM balances WHERE account_id = $1",
      [accountId]
    )
    return rows[0]?.amount ?? 0n
  }

  try {
    await admin.connect()

    const systemId = await createAccount("system", pool)
    const assetId = await createAccount("asset", pool)

    // Publish anything an earlier run left pending, before the cursor is seeded.
    // Otherwise those messages land after it and count as lag this drill did not
    // create, and the assertion fails for a reason unrelated to lag.
    await drainOutbox(pool, producer)

    // Seed the group's cursor at the end of every partition. A consumer commits
    // offsets only for partitions it has read from, so a warm-up message leaves
    // the other two with no cursor — which is not a lag of zero, it is a lag
    // that cannot be computed. setOffsets says it outright, and it is the same
    // tool you would reach for to rewind a group and rebuild a projection.
    const ends = await admin.fetchTopicOffsets(topic)
    await admin.setOffsets({
      groupId: group,
      topic,
      partitions: ends.map((e) => ({
        partition: e.partition,
        offset: e.offset,
      })),
    })

    assert(
      (await lagFor(admin, group, topic)) === 0,
      "the group starts caught up on every partition"
    )

    console.log(`[DRILL] publishing ${BACKLOG} events with no consumer running`)
    for (let i = 0; i < BACKLOG; i++) {
      await postAndRelay(pool, producer, systemId, assetId)
    }

    assert(
      (await lagFor(admin, group, topic)) === BACKLOG,
      `lag built to exactly ${BACKLOG}`
    )
    assert(
      (await balanceOf(assetId)) === 0n,
      "the read model is stale — the write side moved, the projection has not"
    )

    console.log("[DRILL] starting the consumer")
    consumer = await start()

    const expected = AMOUNT * BigInt(BACKLOG)
    await waitFor(
      async () => (await balanceOf(assetId)) === expected,
      "catch-up"
    )

    assert(
      (await lagFor(admin, group, topic)) === 0,
      "lag drained back to zero"
    )
    assert(
      (await balanceOf(assetId)) === expected,
      `the projection caught up (balance ${await balanceOf(assetId)})`
    )

    console.log(
      "\n[DRILL] passed — a paused consumer builds lag and loses nothing"
    )
  } finally {
    await consumer?.disconnect()
    await producer.disconnect()
    await admin.disconnect()
    await shutdown()
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
