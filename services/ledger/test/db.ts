import { inject } from "vitest"
import { Pool, type PoolClient } from "pg"

import { makeTransaction } from "../src/db.js"

let pool: Pool | null = null

export function getTestPool(): Pool {
  pool ??= new Pool({
    connectionString: inject("pgUri"),
    options: "-c idle_in_transaction_session_timeout=2s -c lock_timeout=2s",
  })
  return pool
}

export async function closeTestPool(): Promise<void> {
  await pool?.end()
  pool = null
}

export const testTransaction = <T>(cb: (c: PoolClient) => Promise<T>) =>
  makeTransaction(getTestPool())(cb)
