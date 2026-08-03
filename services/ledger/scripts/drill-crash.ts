import { fileURLToPath } from "node:url"
import { spawn } from "node:child_process"
import type { Pool, PoolClient } from "pg"

import { formatMoney } from "@ledgerrail/money"

import { bootstrap } from "../src/index.js"
import type { Balance } from "../src/types.js"
import { getPool, transaction } from "../src/db.js"
import { randomString } from "../src/utils/helpers.js"
import { rebuildBalances } from "../src/projections/balances/index.js"
import {
  postTransaction,
  type PostTransactionInput,
} from "../src/commands/postTransaction/index.js"

async function createAccount(type: "system" | "asset", pool: Pool) {
  const { rows } = await pool.query<{ id: number }>(
    `INSERT INTO accounts (account_number, currency, type) VALUES ($1, 'PKR', $2) RETURNING id;`,
    [randomString(), type]
  )
  if (!rows[0]?.id) throw new Error(`Account ${type} couldn't be created`)

  return rows[0].id
}

async function getBalances(systemId: number, asset1Id: number, pool: Pool) {
  const { rows: balances } = await pool.query<Balance>(
    "SELECT * FROM balances WHERE account_id = ANY($1)",
    [[systemId, asset1Id]]
  )

  function logBalance(
    balances: Balance[],
    accountId: number,
    type: "System" | "Asset A" | "Asset B"
  ) {
    const balance = balances.find((b) => b.account_id === accountId)?.amount

    console.log(
      `[Balance] ${type} : ${formatMoney({ amount: balance || 0n, currency: "PKR" })} PKR`
    )
  }

  logBalance(balances, systemId, "System")
  logBalance(balances, asset1Id, "Asset A")
}

async function getEvents(idempotencyKey: string, pool: Pool) {
  const { rows: events } = await pool.query<{ n: number }>(
    "SELECT count(*)::int AS n FROM events WHERE idempotency_key = $1",
    [idempotencyKey]
  )

  const { rows: latestGlobalEvent } = await pool.query<{ max: number }>(
    "SELECT MAX(global_seq) FROM events;"
  )

  console.log("Events", events[0]?.n)
  console.log("Events Global Seq", latestGlobalEvent[0]?.max)

  return events[0]?.n
}

function crashOn(client: PoolClient, marker: string): PoolClient {
  return new Proxy(client, {
    get(target, prop, receiver) {
      if (prop !== "query") return Reflect.get(target, prop, receiver)
      return (...args: any[]) => {
        const sql =
          typeof args[0] === "string" ? args[0] : (args[0]?.text ?? "")
        if (sql.includes(marker)) {
          console.log(`💥 killing process before: ${marker}`)
          process.exit(137) // hard kill — no cleanup, no COMMIT
        }
        return (target as any).query(...args)
      }
    },
  })
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
    await transaction((c) =>
      postTransaction(input, crashOn(c, "INSERT INTO balances"))
    )
    throw new Error("unreachable — crashOn should have killed the process")
  }

  const pool = getPool()
  const idempotencyKey = randomString()

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
  if (code !== 137) throw new Error("Child didn't return the correct code")

  // 3. re-read event count + balances from a FRESH connection
  const afterEvents = await getEvents(idempotencyKey, pool)
  await getBalances(systemId, assetId, pool)

  // 4. assert both unchanged, print the comparison
  if (events !== afterEvents)
    throw new Error(
      `expected ${events} events after the crash, found ${afterEvents}`
    )

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

  await pool.end()
}

main()
