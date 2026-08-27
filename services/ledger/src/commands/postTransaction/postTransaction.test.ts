import { Pool, type PoolClient } from "pg"
import { describe, it, expect, beforeAll, afterAll } from "vitest"

import type { Event, Outbox } from "../../types.js"
import { randomString } from "../../utils/helpers.js"
import { createAccounts } from "../../../test/utils.js"
import {
  getTestPool,
  closeTestPool,
  testTransaction,
} from "../../../test/db.js"
import {
  postTransaction,
  type PostTransactionInput,
} from "./postTransaction.js"

afterAll(closeTestPool)

describe("Post Transaction", () => {
  let pool: Pool
  let client: PoolClient

  beforeAll(async () => {
    pool = getTestPool()
    client = await pool.connect()
  })

  afterAll(async () => {
    client?.release()
  })

  it("Correct", async () => {
    const { s, a } = await createAccounts(client)

    const idempotencyKey = randomString()

    const input: PostTransactionInput = {
      idempotencyKey,
      currency: "PKR",
      postings: [
        {
          accountId: s,
          amount: -100n,
        },
        {
          accountId: a,
          amount: 100n,
        },
      ],
    }

    const result = await postTransaction(input, client)

    expect(typeof result.eventId).toBe("string")
    expect(typeof result.globalSeq).toBe("string")

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
    expect(outbox[0]?.global_seq).toBe(BigInt(result.globalSeq))
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

    await expect(postTransaction(input, client)).rejects.toThrow(
      "Invalid accounts"
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

    await expect(postTransaction(input, client)).rejects.toThrow(
      "Invalid currency"
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

    await expect(postTransaction(input, client)).rejects.toThrow(
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

    await expect(postTransaction(input, client)).rejects.toThrow(
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

    await expect(postTransaction(input, client)).rejects.toThrow(
      "Less legs found"
    )
  })

  it("Insufficient Funds", async () => {
    const { s, a } = await createAccounts(client)

    const input: PostTransactionInput = {
      idempotencyKey: randomString(),
      currency: "PKR",
      postings: [
        {
          accountId: s,
          amount: 100n,
        },
        {
          accountId: a,
          amount: -100n,
        },
      ],
    }

    await expect(postTransaction(input, client)).rejects.toThrow(
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

    const funded = await testTransaction((client) =>
      postTransaction(funderInput, client)
    )

    expect(typeof funded.eventId).toBe("string")
    expect(typeof funded.globalSeq).toBe("string")

    const debit = async () => {
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

      return await testTransaction((client) =>
        postTransaction(debitInput, client)
      )
    }

    const result = await Promise.allSettled([debit(), debit()])

    const fulfilled = result.filter((r) => r.status === "fulfilled")
    const rejected = result.filter(
      (r): r is PromiseRejectedResult => r.status === "rejected"
    )

    expect(fulfilled).toHaveLength(1)
    expect(rejected).toHaveLength(1)
    expect(rejected[0]?.reason.message).toBe("Insufficient funds")
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

    const build = async () =>
      testTransaction((client) => postTransaction(input, client))

    // Race condition should pass
    const results = await Promise.all([build(), build()])
    const { globalSeq, eventId } = results[0]

    results.forEach((r) => {
      expect(r.eventId).toBe(eventId)
      expect(r.globalSeq).toBe(globalSeq)
    })

    // Retry should pass
    const result = await build()
    expect(result.eventId).toBe(eventId)
    expect(result.globalSeq).toBe(globalSeq)

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

describe("Idempotency Replay", () => {
  let pool: Pool
  let client: PoolClient

  beforeAll(async () => {
    pool = getTestPool()
    client = await pool.connect()
  })

  afterAll(async () => {
    client?.release()
  })

  it("returns the original result when a key is replayed after the balance moved", async () => {
    const { s, a } = await createAccounts(client)

    const { rows } = await client.query<{ id: number }>(
      `INSERT INTO accounts (account_number, currency, type)
       VALUES ($1, 'PKR', 'asset') RETURNING id;`,
      [randomString()]
    )
    const b = rows[0]!.id

    const transfer = (amount: bigint, idempotencyKey: string) =>
      postTransaction(
        {
          idempotencyKey,
          currency: "PKR",
          postings: [
            { accountId: a, amount: -amount },
            { accountId: b, amount },
          ],
        },
        client
      )

    await postTransaction(
      {
        idempotencyKey: randomString(),
        currency: "PKR",
        postings: [
          { accountId: s, amount: -100n },
          { accountId: a, amount: 100n },
        ],
      },
      client
    )

    const key = randomString()
    const original = await transfer(60n, key)

    // Drain what is left under a different key, so the retry below cannot pass
    // the overdraft guard on its own merits.
    await transfer(40n, randomString())

    const retried = await transfer(60n, key)

    expect(retried).toEqual(original)

    // And it did not move money a second time.
    const { rows: balances } = await client.query<{ posted: bigint }>(
      "SELECT posted FROM account_balances WHERE account_id = $1",
      [b]
    )
    expect(balances[0]?.posted).toBe(100n)
  })
})
