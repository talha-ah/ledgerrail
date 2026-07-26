import { Pool, type PoolClient } from "pg"
import { describe, it, expect, beforeAll, afterAll } from "vitest"

import type { Balance } from "../types.js"
import { getTestPool } from "../../test/db.js"
import { randomString } from "../utils/helpers.js"
import {
  postTransaction,
  type PostTransactionInput,
} from "./postTransaction.js"

describe("Overdraft Race", () => {
  const pool: Pool = getTestPool() // memoized, one worker

  let assetId: number
  let systemId: number

  beforeAll(async () => {
    const sys = await pool.query(
      `
        INSERT INTO accounts (type, account_number, currency)
        VALUES ('system', $1, 'PKR')
        RETURNING id;
      `,
      [randomString()]
    )
    const ast = await pool.query(
      `
        INSERT INTO accounts (type, account_number, currency)
        VALUES ('asset', $1, 'PKR')
        RETURNING id;
      `,
      [randomString()]
    )
    assetId = ast.rows[0].id
    systemId = sys.rows[0].id
  })

  afterAll(async () => await pool?.end())

  it("two concurrent debits overdraw the account", async () => {
    const funderInput: PostTransactionInput = {
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

    const funderClient = await pool.connect()

    await funderClient.query("BEGIN")
    const funded = await postTransaction(funderInput, funderClient)
    await funderClient.query("COMMIT")
    funderClient.release()

    expect(typeof funded.eventId).toBe("string")
    expect(typeof funded.globalSeq).toBe("string")

    // Need two separate connections to own each transaction in parallel
    const a = await pool.connect()
    const b = await pool.connect()

    const debit = async (client: PoolClient) => {
      const debitInput: PostTransactionInput = {
        idempotencyKey: randomString(),
        currency: "PKR",
        postings: [
          {
            accountId: systemId,
            amount: 80n,
          },
          {
            accountId: assetId,
            amount: -80n,
          },
        ],
      }

      await client.query("BEGIN")
      const result = await postTransaction(debitInput, client)
      await client.query("COMMIT")
      client.release()

      return result
    }

    const result = await Promise.allSettled([debit(a), debit(b)])

    for (const res of result) {
      expect(res.status).toBe("fulfilled")
    }

    const { rows: balances } = await pool.query<Balance>(
      "SELECT * FROM balances WHERE account_id = $1",
      [assetId]
    )
    if (!balances[0]) throw new Error("Balance not found")

    expect(BigInt(balances[0].amount)).toBe(-60n) // both debits got through
  })
})
