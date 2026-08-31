/**
 * Stage 2 drill — losing quorum.
 *
 * Kills two of three brokers. The interesting assertion is not that Kafka broke:
 * it is that the ledger did not. postTransaction never touches Kafka, so commands
 * keep committing, the outbox absorbs the backlog, and recovery drains it.
 *
 * Two distinct failures happen at once and are worth naming separately: the
 * controller loses its Raft majority and can no longer commit cluster metadata,
 * and every partition — itself a Raft group of three — loses the majority it
 * needs to elect a leader or commit a write. The second is what the relay hits.
 */
import type { Pool } from "pg"
import * as path from "node:path"
import { fileURLToPath } from "node:url"
import { spawn } from "node:child_process"

import { getPool, transaction } from "../../src/db.js"
import { publishBatch } from "../../src/relay/relay.js"
import { bootstrap, shutdown } from "../../src/index.js"
import { errorMessage } from "../../src/utils/error.js"
import { randomString } from "../../src/utils/helpers.js"
import { postTransaction } from "../../src/commands/postTransaction/index.js"
import {
  getKafka,
  resetKafka,
  type ProducerHandle,
} from "../../src/kafka/index.js"

const AMOUNT = 100n
const DURING_OUTAGE = 3
const DOWN = ["redpanda-1", "redpanda-2"]

// dirname, not the file — a path built off import.meta.url without it is one
// level too deep.
const COMPOSE = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../../infra/docker-compose.yml"
)

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(`[DRILL] FAILED — ${message}`)
  console.log(`[DRILL] ok — ${message}`)
}

function compose(...args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = spawn("docker", ["compose", "-f", COMPOSE, ...args], {
      stdio: "inherit",
    })
    proc.on("exit", (code) =>
      code === 0
        ? resolve()
        : reject(new Error(`docker compose ${args.join(" ")} exited ${code}`))
    )
  })
}

/** Asks the surviving broker about itself, and hands back what it said. */
function rpk(...args: string[]): Promise<{ code: number; out: string }> {
  return new Promise((resolve, reject) => {
    const proc = spawn(
      "docker",
      ["compose", "-f", COMPOSE, "exec", "-T", "redpanda-0", "rpk", ...args],
      { stdio: ["ignore", "pipe", "inherit"] }
    )

    let out = ""
    proc.stdout.on("data", (chunk) => (out += chunk))

    // Deliberately not rejecting on a non-zero code: rpk reports an unhealthy
    // cluster by exiting non-zero, so treating that as a failed command throws
    // away the verdict the drill exists to read.
    proc.on("exit", (code) => resolve({ code: code ?? -1, out }))
    proc.on("error", reject)
  })
}

/**
 * Stopping a container is instant; noticing is not. The survivors must miss
 * enough heartbeats to declare a peer down, so the cluster reports itself
 * perfectly healthy for several seconds after two thirds of it has gone.
 */
async function waitForUnhealthy(ms = 30_000): Promise<string> {
  const started = Date.now()
  const deadline = started + ms
  let last = ""

  while (Date.now() < deadline) {
    const { code, out } = await rpk("cluster", "health")
    last = out

    if (
      code !== 0 ||
      /Healthy:\s+false/i.test(out) ||
      /Nodes down:\s+\[[^\]]+\]/.test(out)
    ) {
      console.log(`[DRILL] cluster noticed after ${Date.now() - started}ms`)
      return out
    }

    await new Promise((r) => setTimeout(r, 1000))
  }

  throw new Error(
    `[DRILL] FAILED — cluster still reports healthy ${ms}ms after losing two of three brokers:\n${last}`
  )
}

/**
 * A producer that gives up quickly. The real relay should retry patiently; a
 * drill that waits out kafkajs's default backoff proves the same thing in two
 * minutes instead of ten seconds.
 */
async function impatientProducer(): Promise<ProducerHandle> {
  const producer = getKafka().producer({
    retry: { retries: 1, initialRetryTime: 300 },
  })
  await producer.connect()

  return {
    async send(topic, key, value) {
      await producer.send({
        topic,
        messages: [{ key, value: JSON.stringify(value) }],
      })
    },
    disconnect: () => producer.disconnect(),
  }
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

async function pending(pool: Pool): Promise<number> {
  const { rows } = await pool.query<{ n: number }>(
    "SELECT COUNT(*)::int AS n FROM outbox WHERE published_at IS NULL"
  )
  return rows[0]?.n ?? 0
}

async function main() {
  await bootstrap()

  let stopped = false
  const pool = getPool()
  let producer = await impatientProducer()

  try {
    // Start from a drained outbox so the counts below are about this drill.
    let n: number
    do {
      n = await publishBatch(pool, producer)
    } while (n > 0)
    assert((await pending(pool)) === 0, "outbox drained before the outage")

    const systemId = await createAccount("system", pool)
    const assetId = await createAccount("asset", pool)

    console.log(
      `[DRILL] stopping ${DOWN.join(", ")} — keeping redpanda-0 alive`
    )
    // Deliberately not the bootstrap broker. Kill the one the client seeds from
    // and you get a connection error, not an observation about quorum.
    await compose("stop", ...DOWN)
    stopped = true

    const health = await waitForUnhealthy()
    console.log(health)

    assert(
      /Healthy:\s+false/i.test(health) ||
        /Nodes down:\s+\[[^\]]+\]/.test(health),
      "the surviving broker reports the cluster unhealthy — quorum lost"
    )

    const keys: string[] = []
    for (let i = 0; i < DURING_OUTAGE; i++) {
      const idempotencyKey = randomString()
      keys.push(idempotencyKey)

      await transaction((client) =>
        postTransaction(
          {
            idempotencyKey,
            currency: "PKR",
            postings: [
              { accountId: systemId, amount: -AMOUNT },
              { accountId: assetId, amount: AMOUNT },
            ],
          },
          client
        )
      )
    }

    // The headline, read back from the database rather than inferred from the
    // absence of a throw: Kafka is unavailable and the ledger accepted every one.
    const { rows: committed } = await pool.query<{ n: number }>(
      "SELECT COUNT(*)::int AS n FROM events WHERE idempotency_key = ANY($1)",
      [keys]
    )

    assert(
      committed[0]?.n === DURING_OUTAGE,
      `${DURING_OUTAGE} transactions committed with the cluster below quorum`
    )
    assert(
      (await pending(pool)) === DURING_OUTAGE,
      `the outbox absorbed all ${DURING_OUTAGE} — nothing was lost, nothing published`
    )

    let failure: unknown = null
    try {
      const published = await publishBatch(pool, producer)
      throw new Error(`expected the publish to fail, but it sent ${published}`)
    } catch (err) {
      failure = err
    }

    // Printed, not swallowed: a catch-all that assumes the cause makes this
    // assertion pass for a Postgres error or a typo just as happily.
    console.log(`[DRILL] publish failed with: ${errorMessage(failure)}`)
    assert(
      failure !== null,
      "the relay could not publish — the partition leader was gone and no election could replace it"
    )
    assert(
      (await pending(pool)) === DURING_OUTAGE,
      "the outbox rows are untouched after the failed publish"
    )

    console.log(`[DRILL] restarting ${DOWN.join(", ")}`)
    await compose("start", ...DOWN)
    stopped = false

    // A fresh producer: the old one's connections were torn down mid-outage.
    await producer.disconnect()
    resetKafka()
    producer = await impatientProducer()

    const deadline = Date.now() + 60_000
    while ((await pending(pool)) > 0 && Date.now() < deadline) {
      try {
        await publishBatch(pool, producer)
      } catch {
        await new Promise((r) => setTimeout(r, 1000))
      }
    }

    assert(
      (await pending(pool)) === 0,
      "the relay drained the backlog once the cluster recovered"
    )

    console.log(
      "\n[DRILL] passed — a broker outage degraded to lag, not to lost or refused writes"
    )
  } finally {
    // Never leave the cluster half dead: the next thing anyone runs would fail
    // for reasons that have nothing to do with it.
    if (stopped) await compose("start", ...DOWN).catch(() => {})
    await producer.disconnect().catch(() => {})
    await shutdown()
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
