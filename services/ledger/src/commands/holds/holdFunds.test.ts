import { type Pool, type PoolClient } from "pg"
import { describe, it, expect, beforeAll, afterAll } from "vitest"

import type { Hold } from "../../types.js"
import { holdFunds } from "./holdFunds.js"
import { randomString } from "../../utils/helpers.js"
import { createAccounts } from "../../../test/utils.js"
import { postTransaction } from "../postTransaction/index.js"
import { getTestPool, closeTestPool } from "../../../test/db.js"
import { captureHold, releaseHold, holdKey } from "./settleHold.js"

const HOUR = 60 * 60 * 1000
const inAnHour = () => new Date(Date.now() + HOUR)
const anHourAgo = () => new Date(Date.now() - HOUR)

describe("Holds", () => {
  let pool: Pool
  let client: PoolClient
  let heldAccountId: number

  beforeAll(async () => {
    pool = getTestPool()
    client = await pool.connect()

    const { rows } = await client.query<{ id: number }>(
      "SELECT id FROM accounts WHERE account_number = 'HELD:PKR'"
    )

    if (!rows[0]) throw new Error("the holds migration did not seed HELD:PKR")
    heldAccountId = rows[0].id
  })

  afterAll(async () => {
    client?.release()
    await closeTestPool()
  })

  async function fundedAccount(amount: bigint) {
    const { s, a } = await createAccounts(client)

    await postTransaction(
      {
        idempotencyKey: randomString(),
        currency: "PKR",
        postings: [
          { accountId: s, amount: -amount },
          { accountId: a, amount: amount },
        ],
      },
      client
    )

    return { systemId: s, accountId: a }
  }

  async function availableOf(accountId: number): Promise<bigint> {
    const { rows } = await client.query<{ pending: bigint; posted: bigint }>(
      `SELECT pending, posted FROM account_balances WHERE account_id = $1`,
      [accountId]
    )

    return (rows[0]?.pending ?? 0n) + (rows[0]?.posted ?? 0n)
  }

  async function reserveBalance(): Promise<bigint> {
    // HELD:PKR is shared by every test in this file, so assertions on it must be
    // deltas rather than absolutes.
    return availableOf(heldAccountId)
  }

  async function holdRow(holdId: string): Promise<Hold> {
    const { rows } = await client.query<Hold>(
      "SELECT * FROM holds WHERE hold_id = $1",
      [holdId]
    )
    if (!rows[0]) throw new Error(`no hold row for ${holdId}`)
    return rows[0]
  }

  function hold(accountId: number, amount: bigint, expiresAt = inAnHour()) {
    return holdFunds(
      {
        accountId,
        amount,
        expiresAt,
        currency: "PKR",
        idempotencyKey: randomString(),
      },
      client
    )
  }

  it("moves the reserved amount into the reserve account", async () => {
    const { accountId } = await fundedAccount(100n)
    const reserveBefore = await reserveBalance()

    const { holdId } = await hold(accountId, 80n)

    expect(await availableOf(accountId)).toBe(20n)
    expect((await reserveBalance()) - reserveBefore).toBe(80n)

    const row = await holdRow(holdId)
    expect(row.status).toBe("held")
    expect(row.amount).toBe(80n)
    expect(row.hold_account_id).toBe(heldAccountId)
    expect(row.settled_event_id).toBeNull()
  })

  it("writes the hold to the journal as a balanced event", async () => {
    const { accountId } = await fundedAccount(100n)
    const { holdId } = await hold(accountId, 80n)

    const { held_event_id } = await holdRow(holdId)

    const { rows } = await client.query<{
      event_type: string
      event_version: number
      payload: { holdId: string; postings: { amount: string }[] }
    }>(
      "SELECT event_type, event_version, payload FROM events WHERE event_id = $1",
      [held_event_id]
    )

    const event = rows[0]
    expect(event?.event_type).toBe("FundsHeld")
    expect(event?.event_version).toBe(1)
    expect(event?.payload.holdId).toBe(holdId)

    // The reason a hold is an event rather than a column: it is a posting, and
    // it obeys the same invariant as every other posting.
    const total = (event?.payload.postings ?? []).reduce(
      (sum, leg) => sum + BigInt(leg.amount),
      0n
    )
    expect(total).toBe(0n)
  })

  it("refuses a transfer the held funds would have covered", async () => {
    const { systemId, accountId } = await fundedAccount(100n)

    await hold(accountId, 80n)

    await expect(
      postTransaction(
        {
          idempotencyKey: randomString(),
          currency: "PKR",
          postings: [
            { accountId, amount: -40n },
            { accountId: systemId, amount: 40n },
          ],
        },
        client
      )
    ).rejects.toThrow("Insufficient funds")
  })

  it("refuses a second hold the balance cannot cover", async () => {
    const { accountId } = await fundedAccount(100n)

    const a = await pool.connect()
    const b = await pool.connect()

    try {
      // Explicit transactions, on separate connections. A row lock lives exactly
      // as long as its transaction, so in autocommit the FOR UPDATE inside the
      // guard would be released within its own statement and protect nothing —
      // and one connection processes statements sequentially, so a race needs two.
      await a.query("BEGIN")
      await b.query("BEGIN")

      const first = await holdFunds(
        {
          accountId,
          amount: 80n,
          currency: "PKR",
          expiresAt: inAnHour(),
          idempotencyKey: randomString(),
        },
        a
      )

      // Started, not awaited: b must block on the lock a is holding.
      const second = holdFunds(
        {
          accountId,
          amount: 80n,
          currency: "PKR",
          expiresAt: inAnHour(),
          idempotencyKey: randomString(),
        },
        b
      ).then(
        () => "committed" as const,
        (err: Error) => err
      )

      await new Promise((resolve) => setTimeout(resolve, 200))
      await a.query("COMMIT")

      const outcome = await second

      // The point: b did not read a stale balance and approve a second hold. It
      // waited, re-read what a had committed, and refused.
      expect(outcome).toBeInstanceOf(Error)
      expect((outcome as Error).message).toMatch(/Insufficient funds/)

      await b.query("ROLLBACK")

      expect(first.holdId).toBeTruthy()
      expect(await availableOf(accountId)).toBe(20n)
    } finally {
      a.release()
      b.release()
    }
  })

  it("holds once when the same idempotency key is used twice", async () => {
    const { accountId } = await fundedAccount(100n)
    const idempotencyKey = randomString()

    const input = {
      accountId,
      amount: 80n,
      currency: "PKR" as const,
      expiresAt: inAnHour(),
      idempotencyKey,
    }

    const first = await holdFunds(input, client)
    const second = await holdFunds(input, client)

    expect(second.holdId).toBe(first.holdId)
    expect(second.eventId).toBe(first.eventId)

    // Money reserved once, not twice.
    expect(await availableOf(accountId)).toBe(20n)

    const { rows } = await client.query<{ n: number }>(
      "SELECT COUNT(*)::int AS n FROM holds WHERE account_id = $1",
      [accountId]
    )
    expect(rows[0]?.n).toBe(1)
  })

  it("captures a hold into the destination account", async () => {
    const { accountId } = await fundedAccount(100n)
    const { a: destinationId } = await createAccounts(client)

    const { holdId } = await hold(accountId, 80n)
    const reserveBefore = await reserveBalance()

    await captureHold(holdId, destinationId, randomString(), client)

    expect(await availableOf(destinationId)).toBe(80n)
    expect((await reserveBalance()) - reserveBefore).toBe(-80n)

    // The customer's balance does not move again: the money left when it was
    // held. Capture moves it out of the reserve, not out of the account.
    expect(await availableOf(accountId)).toBe(20n)

    const row = await holdRow(holdId)
    expect(row.status).toBe("captured")
    expect(row.settled_event_id).not.toBeNull()
  })

  it("releases a hold back to the account it came from", async () => {
    const { accountId } = await fundedAccount(100n)

    const { holdId } = await hold(accountId, 80n)
    expect(await availableOf(accountId)).toBe(20n)

    await releaseHold(holdId, randomString(), client)

    expect(await availableOf(accountId)).toBe(100n)
    expect((await holdRow(holdId)).status).toBe("released")
  })

  it("refuses to settle a hold twice", async () => {
    const { accountId } = await fundedAccount(100n)
    const { a: destinationId } = await createAccounts(client)

    const { holdId } = await hold(accountId, 80n)
    await captureHold(holdId, destinationId, randomString(), client)

    // A different idempotency key, so this is not caught by event dedup — it is
    // the hold's own status transition refusing a second consumer.
    await expect(
      captureHold(holdId, destinationId, randomString(), client)
    ).rejects.toThrow("Hold is not available to settle")

    await expect(releaseHold(holdId, randomString(), client)).rejects.toThrow(
      "Hold is not available to settle"
    )

    expect(await availableOf(destinationId)).toBe(80n)
  })

  it("refuses to capture an expired hold", async () => {
    const { accountId } = await fundedAccount(100n)
    const { a: destinationId } = await createAccounts(client)

    const { holdId } = await hold(accountId, 80n, anHourAgo())

    await expect(
      captureHold(holdId, destinationId, randomString(), client)
    ).rejects.toThrow("Hold has expired")

    // Still held, still reserved — expiry refuses the capture, it does not undo
    // the hold. Returning the money is the sweeper's job, and the sweeper is 3.4.
    expect((await holdRow(holdId)).status).toBe("held")
    expect(await availableOf(accountId)).toBe(20n)
  })

  it("releases an expired hold it would refuse to capture", async () => {
    const { accountId } = await fundedAccount(100n)

    const { holdId } = await hold(accountId, 80n, anHourAgo())
    expect(await availableOf(accountId)).toBe(20n)

    // Expiry bounds how long money may be *held*, not how long it may be given
    // back. The asymmetry is load-bearing from 3.4: a capture that fails on
    // expiry abandons to `compensating`, whose only move is this release — and
    // if it refused for the same reason the money would be stranded by the very
    // deadline that exists to protect it.
    await releaseHold(holdId, randomString(), client)

    expect((await holdRow(holdId)).status).toBe("released")
    expect(await availableOf(accountId)).toBe(100n)
  })

  it("returns the original receipt when a release is replayed", async () => {
    const { accountId } = await fundedAccount(100n)
    const { holdId } = await hold(accountId, 80n)

    const key = holdKey(holdId, "release")
    const first = await releaseHold(holdId, key, client)
    const reserveAfterFirst = await reserveBalance()

    // The same key from a second caller — a compensating payment and the sweeper
    // both derive it. Latent since 3.1: the hold was claimed before the replay
    // was looked up, so this threw "Hold is not available to settle" on a call
    // that had already succeeded. The same ordering bug 3.1 fixed one file over
    // in postTransaction, left behind here because nothing called it twice.
    const second = await releaseHold(holdId, key, client)

    expect(second.eventId).toBe(first.eventId)
    expect(second.replayed).toBe(true)

    // And nothing moved the second time.
    expect(await reserveBalance()).toBe(reserveAfterFirst)
    expect(await availableOf(accountId)).toBe(100n)

    const { rows } = await client.query<{ n: number }>(
      "SELECT COUNT(*)::int AS n FROM events WHERE idempotency_key = $1",
      [key]
    )
    expect(rows[0]?.n).toBe(1)
  })
})
