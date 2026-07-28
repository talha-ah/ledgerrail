import type { Pool, PoolClient } from "pg"
import { describe, it, beforeAll, afterAll, expect } from "vitest"

import type { Balance } from "../../types.js"
import { getTestPool } from "../../../test/db.js"
import { randomString } from "../../utils/helpers.js"
import { applyToBalances, rebuildBalances } from "./balances.js"
import {
  postTransaction,
  type PostTransactionInput,
} from "../../commands/postTransaction/index.js"

describe("Apply to Balances", () => {
  const pool: Pool = getTestPool()

  let assetId: number
  let systemId: number
  let client: PoolClient

  const TRANSACTIONS: bigint[] = [20n, 100n, 70n, -100n, 200n, 10n, 500n, 15n]

  beforeAll(async () => {
    client = await pool.connect()

    const s = await pool.query(
      `INSERT INTO accounts (account_number, currency, type) VALUES ($1, 'PKR', 'system') RETURNING id;`,
      [randomString()]
    )
    const a = await pool.query(
      `INSERT INTO accounts (account_number, currency, type) VALUES ($1, 'PKR', 'asset') RETURNING id;`,
      [randomString()]
    )

    assetId = a.rows[0].id
    systemId = s.rows[0].id
  })

  afterAll(async () => {
    client.release()
    await pool?.end()
  })

  it("success", async () => {
    for (const transaction of TRANSACTIONS) {
      const input: PostTransactionInput = {
        idempotencyKey: randomString(),
        currency: "PKR",
        postings: [
          {
            accountId: systemId,
            amount: BigInt(-transaction),
          },
          {
            accountId: assetId,
            amount: BigInt(transaction),
          },
        ],
      }

      try {
        await client.query("BEGIN")
        await postTransaction(input, client)
        await client.query("COMMIT")
      } catch (err) {
        await client.query("ROLLBACK")
        throw err
      }
    }

    const { rows: beforeBalances } = await client.query<Balance>(
      "SELECT * FROM balances"
    )

    for (const balance of beforeBalances) {
      if (balance.account_id === systemId) {
        expect(BigInt(balance.amount)).toBe(-815n)
      } else if (balance.account_id === assetId) {
        expect(BigInt(balance.amount)).toBe(815n)
      }
    }

    try {
      await client.query("BEGIN")
      await rebuildBalances(client)
      await client.query("COMMIT")
    } catch (err) {
      console.log("err", err)
      await client.query("ROLLBACK")
      throw err
    }

    const { rows: afterBalances } = await client.query<Balance>(
      "SELECT * FROM balances"
    )

    expect(beforeBalances.length).toBe(afterBalances.length)

    for (const balance of afterBalances) {
      if (balance.account_id === systemId) {
        expect(BigInt(balance.amount)).toBe(-815n)
      } else if (balance.account_id === assetId) {
        expect(BigInt(balance.amount)).toBe(815n)
      }
    }
  })
})

describe("Rebuild Balances", () => {
  const pool: Pool = getTestPool()

  let assetId: number
  let systemId: number
  let client: PoolClient

  const TRANSACTIONS: bigint[] = [20n, 100n, 70n, -100n, 200n, 10n, 500n, 15n]

  beforeAll(async () => {
    client = await pool.connect()

    const s = await pool.query(
      `INSERT INTO accounts (account_number, currency, type) VALUES ($1, 'PKR', 'system') RETURNING id;`,
      [randomString()]
    )
    const a = await pool.query(
      `INSERT INTO accounts (account_number, currency, type) VALUES ($1, 'PKR', 'asset') RETURNING id;`,
      [randomString()]
    )

    assetId = a.rows[0].id
    systemId = s.rows[0].id
  })

  afterAll(async () => {
    client.release()
    await pool?.end()
  })

  it("success", async () => {
    for (const transaction of TRANSACTIONS) {
      const input: PostTransactionInput = {
        idempotencyKey: randomString(),
        currency: "PKR",
        postings: [
          {
            accountId: systemId,
            amount: BigInt(-transaction),
          },
          {
            accountId: assetId,
            amount: BigInt(transaction),
          },
        ],
      }

      try {
        await client.query("BEGIN")
        await postTransaction(input, client)
        await client.query("COMMIT")
      } catch (err) {
        await client.query("ROLLBACK")
        throw err
      }
    }

    const { rows: beforeBalances } = await client.query<Balance>(
      "SELECT * FROM balances"
    )

    for (const balance of beforeBalances) {
      if (balance.account_id === systemId) {
        expect(BigInt(balance.amount)).toBe(-815n)
      } else if (balance.account_id === assetId) {
        expect(BigInt(balance.amount)).toBe(815n)
      }
    }

    try {
      await client.query("BEGIN")
      await rebuildBalances(client)
      await client.query("COMMIT")
    } catch (err) {
      console.log("err", err)
      await client.query("ROLLBACK")
      throw err
    }

    const { rows: afterBalances } = await client.query<Balance>(
      "SELECT * FROM balances"
    )

    expect(beforeBalances.length).toBe(afterBalances.length)

    for (const balance of afterBalances) {
      if (balance.account_id === systemId) {
        expect(BigInt(balance.amount)).toBe(-815n)
      } else if (balance.account_id === assetId) {
        expect(BigInt(balance.amount)).toBe(815n)
      }
    }
  })
})
