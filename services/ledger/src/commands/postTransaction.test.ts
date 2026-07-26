import { describe, it, expect } from "vitest"

import type { Balance, Event } from "../types.js"
import { randomString } from "../utils/helpers.js"
import { useRollbackClient } from "../../test/db.js"
import {
  postTransaction,
  type PostTransactionInput,
} from "./postTransaction.js"

describe("PostTransaction", () => {
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

    const { rows: events } = await client.query<Event>(
      `SELECT * FROM events WHERE idempotency_key = $1`,
      [idempotencyKey]
    )

    expect(events[0]?.event_id).toBe(result.eventId)
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
