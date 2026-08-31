import { fileURLToPath } from "node:url"
import { spawn } from "node:child_process"
import type { Pool, PoolClient } from "pg"

import { formatMoney } from "@ledgerrail/money"

import { bootstrap } from "../../src/index.js"
import { getPool, transaction } from "../../src/db.js"
import { randomString } from "../../src/utils/helpers.js"
import { rebuildBalances } from "../../src/projections/balanceProjection/index.js"
import {
  postTransaction,
  type PostTransactionInput,
} from "../../src/commands/postTransaction/index.js"

async function createAccount(type: "system" | "asset", pool: Pool) {
  const { rows } = await pool.query<{ id: number }>(
    `INSERT INTO accounts (account_number, currency, type) VALUES ($1, 'PKR', $2) RETURNING id;`,
    [randomString(), type]
  )
  if (!rows[0]?.id) throw new Error(`Account ${type} couldn't be created`)

  return rows[0].id
}

async function getBalances(systemId: number, assetId: number, pool: Pool) {
  // The write side, not `balances`. The command stopped touching the read model
  // in 2.3 and no consumer runs during this drill, so reading `balances` here
  // printed 0.00 for everything — a healthy-looking ledger that shows nothing.
  const { rows } = await pool.query<{ account_id: number; posted: bigint }>(
    "SELECT account_id, posted FROM account_balances WHERE account_id = ANY($1)",
    [[systemId, assetId]]
  )

  function logBalance(accountId: number, label: "System" | "Asset A") {
    const posted = rows.find((b) => b.account_id === accountId)?.posted

    console.log(
      `[Balance] ${label} : ${formatMoney({ amount: posted ?? 0n, currency: "PKR" })} PKR`
    )
  }

  logBalance(systemId, "System")
  logBalance(assetId, "Asset A")
}

async function getEvents(idempotencyKey: string, pool: Pool) {
  const { rows: events } = await pool.query<{ n: number }>(
    "SELECT count(*)::int AS n FROM events WHERE idempotency_key = $1",
    [idempotencyKey]
  )

  // MAX of a BIGINT is a BIGINT, so this arrives as a bigint and prints as "57n"
  // unless it is stringified. An aggregate has its own type, not the column's.
  const { rows: latestGlobalEvent } = await pool.query<{ max: bigint | null }>(
    "SELECT MAX(global_seq) AS max FROM events;"
  )

  console.log("Events", events[0]?.n)
  console.log("Events Global Seq", String(latestGlobalEvent[0]?.max ?? 0))

  return events[0]?.n
}

/**
 * Wraps the injected client so the process dies on a chosen statement, and keeps
 * a record of what it saw. The record is the difference between "nothing
 * crashed" and "the injection point moved" — two failures that look identical
 * from outside, and the second of which is how this drill silently stopped
 * testing anything when the command started writing a differently named table.
 */
function crashOn(client: PoolClient, marker: string) {
  const seen: string[] = []

  const proxy = new Proxy(client, {
    get(target, prop, receiver) {
      if (prop !== "query") return Reflect.get(target, prop, receiver)

      return (...args: any[]) => {
        const sql =
          typeof args[0] === "string" ? args[0] : (args[0]?.text ?? "")

        seen.push(sql.trim().split("\n")[0]?.slice(0, 60) ?? "")

        if (sql.includes(marker)) {
          console.log(`💥 killing process before: ${marker}`)
          process.exit(137) // hard kill — no cleanup, no COMMIT
        }

        return (target as any).query(...args)
      }
    },
  })

  return { proxy, seen }
}

async function main() {
  // Bootstrap application
  await bootstrap()

  if (process.argv.includes("--child")) {
    console.log(
      `[Child] Hello from inside the child process! (PID: ${process.pid})`
    )

    const [systemId, assetId, idempotencyKey] = process.argv.slice(3)
    if (!systemId || !assetId || !idempotencyKey) {
      throw new Error("Invalid inputs received")
    }

    const input: PostTransactionInput = {
      idempotencyKey,
      currency: "PKR",
      postings: [
        { accountId: Number(systemId), amount: -100n },
        { accountId: Number(assetId), amount: 100n },
      ],
    }

    console.log("Initiating transaction in child process")

    // The balance write, which happens after the event and outbox inserts and
    // before the COMMIT — so the process dies with both written and neither
    // durable. `balances` was the marker until 2.3 moved the write to
    // account_balances, after which it matched nothing at all.
    const marker = "INSERT INTO account_balances"

    await transaction((c) => {
      const { proxy, seen } = crashOn(c, marker)

      return postTransaction(input, proxy).then(() => {
        throw new Error(
          `crashOn never matched "${marker}" - statements seen\n ${seen.join("\n ")}`
        )
      })
    })
  }

  const pool = getPool()
  const idempotencyKey = randomString()

  try {
    console.log("Creating accounts")
    const systemId = await createAccount("system", pool)
    const assetId = await createAccount("asset", pool)

    if (!systemId || !assetId) {
      throw new Error("Accounts could not be created")
    }

    // 1. record: event count + balances
    const events = await getEvents(idempotencyKey, pool)
    await getBalances(systemId, assetId, pool)

    const { rows: beforeOutbox } = await pool.query<{ n: number }>(
      "SELECT COUNT(*)::int as n FROM outbox"
    )

    // 2. spawn self with --child, await its exit (expect a non-zero code)
    console.log(`[Parent] Spawning myself (PID: ${process.pid})`)
    const child = spawn(
      "tsx",
      [
        fileURLToPath(import.meta.url),
        "--child",
        String(systemId),
        String(assetId),
        idempotencyKey,
      ],
      {
        stdio: "inherit",
      }
    )
    const code = await new Promise<number>((resolve) => {
      child.on("exit", (c) => resolve(c ?? -1))
    })
    console.log(`[Parent] child exited with ${code}`)

    // The code we chose, not merely non-zero: a child that dies on an
    // unresolvable import also exits non-zero and looks like a passing drill.
    if (code !== 137) throw new Error("Child didn't return the correct code")

    // 3. re-read event count + balances from a FRESH connection
    const afterEvents = await getEvents(idempotencyKey, pool)
    await getBalances(systemId, assetId, pool)

    // 4. assert both unchanged, print the comparison
    if (events !== afterEvents) {
      throw new Error(
        `expected ${events} events after the crash, found ${afterEvents}`
      )
    }

    const { rows: afterOutbox } = await pool.query<{ n: number }>(
      "SELECT COUNT(*)::int as n FROM outbox"
    )
    if (beforeOutbox[0]?.n !== afterOutbox[0]?.n) {
      throw new Error(
        `expected ${beforeOutbox[0]?.n} outbox after the crash, found ${afterOutbox[0]?.n}`
      )
    }

    // 5. run rebuildBalances and show state is still correct
    console.log("Rebuilding balances")
    await transaction((client) => rebuildBalances(client))

    await getEvents(idempotencyKey, pool)
    await getBalances(systemId, assetId, pool)

    console.log("\n[DRILL] passed — no partial posting survived the crash")
  } finally {
    // In finally: an assertion above that throws would otherwise leave the pool
    // open, and pool.end() waiting on a checked-out client is how a failing
    // drill turns into a hanging one.
    await pool.end()
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
