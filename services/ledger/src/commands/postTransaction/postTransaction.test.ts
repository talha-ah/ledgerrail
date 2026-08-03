import { Pool, type PoolClient } from "pg"
import { describe, it, expect, beforeAll, afterAll } from "vitest"

import { getTestPool } from "../../../test/db.js"
import { randomString } from "../../utils/helpers.js"
import { useRollbackClient } from "../../../test/db.js"
import type { Balance, Event, Outbox } from "../../types.js"
import {
  postTransaction,
  type PostTransactionInput,
} from "./postTransaction.js"

describe("Post Transaction", () => {
  const getClient = useRollbackClient()

  it("Correct", async () => {
    const idempotencyKey = randomString()

    const input: PostTransactionInput = {
      idempotencyKey,
      currency: "PKR",
      postings: [
        {
          accountId: 1,
          amount: -100n,
        },
        {
          accountId: 2,
          amount: 100n,
        },
      ],
    }

    const client = getClient()

    const { rows: balances } = await client.query<Balance>(
      `SELECT * FROM balances WHERE account_id = ANY($1)`,
      [input.postings.map((p) => p.accountId)]
    )

    const result = await postTransaction(input, client)

    expect(typeof result.eventId).toBe("string")
    expect(typeof result.globalSeq).toBe("string")

    for (const posting of input.postings) {
      const { rows: balance } = await client.query<Balance>(
        `SELECT * FROM balances WHERE account_id = $1`,
        [posting.accountId]
      )

      const before =
        balances.find((b) => b.account_id === posting.accountId)?.amount || 0n

      const after = balance[0]
      if (!after) {
        throw new Error(`no balance row for account ${posting.accountId}`)
      }

      expect(BigInt(before) + posting.amount).toBe(BigInt(after.amount))
    }

    const { rows: events } = await client.query<Pick<Event, "event_id">>(
      `SELECT * FROM events WHERE idempotency_key = $1`,
      [idempotencyKey]
    )

    expect(events[0]?.event_id).toBe(result.eventId)

    const { rows: outbox } = await client.query<Outbox>(
      `SELECT * FROM outbox WHERE event_id = $1`,
      [result.eventId]
    )

    expect(outbox).toHaveLength(1)
    expect(outbox[0]?.published_at).toBeNull()
    expect(outbox[0]?.partition_key).toBe(`transaction:${result.eventId}`)
    expect(outbox[0]?.global_seq).toBe(result.globalSeq)
  })

  it("Invalid Accounts", async () => {
    const input: PostTransactionInput = {
      idempotencyKey: randomString(),
      currency: "USD",
      postings: [
        {
          accountId: 100,
          amount: -100n,
        },
        {
          accountId: 2,
          amount: 100n,
        },
      ],
    }

    await expect(postTransaction(input, getClient())).rejects.toThrow(
      "Invalid Accounts"
    )
  })

  it("Invalid currency", async () => {
    const input: PostTransactionInput = {
      idempotencyKey: randomString(),
      currency: "USD",
      postings: [
        {
          accountId: 1,
          amount: -100n,
        },
        {
          accountId: 2,
          amount: 100n,
        },
      ],
    }

    await expect(postTransaction(input, getClient())).rejects.toThrow(
      "Invalid Currency"
    )
  })

  it("Invalid Amount", async () => {
    const input: PostTransactionInput = {
      idempotencyKey: randomString(),
      currency: "USD",
      postings: [
        {
          accountId: 1,
          amount: 0n,
        },
        {
          accountId: 2,
          amount: 100n,
        },
      ],
    }

    await expect(postTransaction(input, getClient())).rejects.toThrow(
      "Invalid legs found"
    )
  })

  it("Mismatched amounts", async () => {
    const input: PostTransactionInput = {
      idempotencyKey: randomString(),
      currency: "PKR",
      postings: [
        {
          accountId: 1,
          amount: -100n,
        },
        {
          accountId: 2,
          amount: 200n,
        },
      ],
    }

    await expect(postTransaction(input, getClient())).rejects.toThrow(
      "Invalid transaction amounts"
    )
  })

  it("Invalid Postings", async () => {
    const input: PostTransactionInput = {
      idempotencyKey: randomString(),
      currency: "USD",
      postings: [
        {
          accountId: 1,
          amount: 100n,
        },
      ],
    }

    await expect(postTransaction(input, getClient())).rejects.toThrow(
      "Less legs found"
    )
  })

  it("Insufficient Funds", async () => {
    const input: PostTransactionInput = {
      idempotencyKey: randomString(),
      currency: "PKR",
      postings: [
        {
          accountId: 1,
          amount: 100n,
        },
        {
          accountId: 2,
          amount: -100n,
        },
      ],
    }

    await expect(postTransaction(input, getClient())).rejects.toThrow(
      "Insufficient funds"
    )
  })
})

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

  it("concurrent debits cannot overdraw", async () => {
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
      try {
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

        return result
      } catch (err) {
        await client.query("ROLLBACK")
        throw err
      } finally {
        client.release()
      }
    }

    const result = await Promise.allSettled([debit(a), debit(b)])

    const fulfilled = result.filter((r) => r.status === "fulfilled")
    const rejected = result.filter(
      (r): r is PromiseRejectedResult => r.status === "rejected"
    )

    expect(fulfilled).toHaveLength(1)
    expect(rejected).toHaveLength(1)
    expect(rejected[0]?.reason.message).toBe("Insufficient funds")

    const { rows: balances } = await pool.query<Balance>(
      "SELECT * FROM balances WHERE account_id = $1",
      [assetId]
    )
    if (!balances[0]) throw new Error("Balance not found")

    expect(BigInt(balances[0].amount)).toBe(20n)
  })
})

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

    const { rows: outbox } = await pool.query<{ n: number }>(
      `SELECT COUNT(*)::int as n FROM outbox WHERE event_id = $1`,
      [result.eventId]
    )

    expect(outbox[0]?.n).toBe(1) // exactly one intent to publish, ever
  })
})
