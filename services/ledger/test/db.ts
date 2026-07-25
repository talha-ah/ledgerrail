import { Pool, type PoolClient } from "pg"
import { inject, beforeEach, afterEach, afterAll } from "vitest"

let pool: Pool

export function useRollbackClient() {
  let client: PoolClient

  beforeEach(async () => {
    pool ??= new Pool({ connectionString: inject("pgUri") })
    client = await pool.connect()
    await client.query("BEGIN")
  })

  afterEach(async () => {
    await client.query("ROLLBACK") // every write this test made vanishies
    client.release()
  })

  afterAll(async () => {
    await pool?.end()
  })

  return () => client
}
