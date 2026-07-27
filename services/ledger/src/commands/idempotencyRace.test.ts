import type { Pool, PoolClient } from "pg"
import { describe, it, beforeAll, afterAll, expect } from "vitest"

import type { Balance } from "../types.js"
import { getTestPool } from "../../test/db.js"
import { randomString } from "../utils/helpers.js"
import {
  postTransaction,
  type PostTransactionInput,
} from "./postTransaction.js"

describe("Idempotency Race", () => {
  let pool: Pool = getTestPool()

  let assetId: number
  let systemId: number

  beforeAll(async () => {
    const s = await pool.query(
      `
      INSERT INTO accounts (account_number, currency, type)
      VALUES ($1, 'PKR', 'system')
      RETURNING id;
      `,
      [randomString()]
    )
    const a = await pool.query(
      `
      INSERT INTO accounts (account_number, currency, type)
      VALUES ($1, 'PKR', 'asset')
      RETURNING id;
      `,
      [randomString()]
    )

    assetId = a.rows[0].id
    systemId = s.rows[0].id
  })

  afterAll(async () => {
    await pool?.end()
  })

  it("Only one event should work and update balance", async () => {
    const input: PostTransactionInput = {
      idempotencyKey: randomString(),
      currency: "PKR",
      postings: [
        {
          accountId: systemId,
          amount: -100n,
        },
        {
          accountId: assetId,
          amount: 100n,
        },
      ],
    }

    const a = await pool.connect()
    const b = await pool.connect()

    const build = async (client: PoolClient) => {
      try {
        await client.query("BEGIN")
        const result = await postTransaction(input, client)
        await client.query("COMMIT")
        return result
      } catch (err) {
        await client.query("ROLLBACK")
        throw err
      } finally {
        client.release()
      }
    }

    // Race condition should pass
    const results = await Promise.all([build(a), build(b)])
    const { globalSeq, eventId } = results[0]

    results.forEach((r) => {
      expect(r.eventId).toBe(eventId)
      expect(r.globalSeq).toBe(globalSeq)
    })

    const { rows: beforeBalances } = await pool.query<Balance>(
      "SELECT * FROM balances WHERE account_id = ANY($1)",
      [[systemId, assetId]]
    )

    const balances = Object.fromEntries(
      beforeBalances.map((b) => [b.account_id, b.amount])
    )

    input.postings.forEach((p) =>
      expect(p.amount).toBe(BigInt(balances[p.accountId]!))
    )

    // Retry should pass
    const result = await build(await pool.connect())
    expect(result.eventId).toBe(eventId)
    expect(result.globalSeq).toBe(globalSeq)

    const { rows: afterBalances } = await pool.query<Balance>(
      "SELECT * FROM balances WHERE account_id = ANY($1)",
      [[systemId, assetId]]
    )

    afterBalances.forEach((b) => expect(b.amount).toBe(balances[b.account_id]))

    const { rows: events } = await pool.query(
      `SELECT count(*)::int AS n FROM events WHERE idempotency_key = $1`,
      [input.idempotencyKey]
    )
    expect(events[0].n).toBe(1) // exactly one event, ever
  })
})
