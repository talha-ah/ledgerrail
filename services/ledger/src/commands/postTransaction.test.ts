import { describe, it, expect } from "vitest"

import { useRollbackClient } from "../../test/db"
import { postTransaction, type PostTransactionInput } from "./postTransaction"

describe("PostTransaction", () => {
  const getClient = useRollbackClient()

  it("Correct", async () => {
    const input: PostTransactionInput = {
      idempotencyKey: "1290129012adf",
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

    const client = getClient()

    const result = await postTransaction(input, client)

    expect(typeof result.eventId).toBe("string")
    expect(typeof result.globalSeq).toBe("string")

    for (const posting of input.postings) {
      const { rows: balance } = await client.query(
        `SELECT * FROM balances WHERE account_id = $1`,
        [posting.accountId]
      )

      expect(balance[0]).toBeDefined()
      expect(BigInt(balance[0].amount)).toBe(posting.amount)
    }

    const { rows: events } = await client.query(`SELECT * FROM events`)
    expect(events.length).toBe(1)
  })

  it("Invalid Accounts", async () => {
    const input: PostTransactionInput = {
      idempotencyKey: "1290129012adf",
      currency: "USD",
      postings: [
        {
          accountId: 100,
          amount: 100n,
        },
        {
          accountId: 2,
          amount: -100n,
        },
      ],
    }

    await expect(postTransaction(input, getClient())).rejects.toThrow(
      "Invalid Accounts"
    )
  })

  it("Invalid currency", async () => {
    const input: PostTransactionInput = {
      idempotencyKey: "1290129012adf",
      currency: "USD",
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
      "Invalid Currency"
    )
  })

  it("Mismatched amounts", async () => {
    const input: PostTransactionInput = {
      idempotencyKey: "1290129012adf",
      currency: "PKR",
      postings: [
        {
          accountId: 1,
          amount: 100n,
        },
        {
          accountId: 2,
          amount: -200n,
        },
      ],
    }

    await expect(postTransaction(input, getClient())).rejects.toThrow(
      "Invalid transaction amounts"
    )
  })

  it("Invalid Amount", async () => {
    const input: PostTransactionInput = {
      idempotencyKey: "1290129012adf",
      currency: "USD",
      postings: [
        {
          accountId: 1,
          amount: 100n,
        },
        {
          accountId: 2,
          amount: 0n,
        },
      ],
    }

    await expect(postTransaction(input, getClient())).rejects.toThrow(
      "Invalid legs found"
    )
  })

  it("Invalid Postings", async () => {
    const input: PostTransactionInput = {
      idempotencyKey: "1290129012adf",
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
})
