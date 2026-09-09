import { Pool, type PoolClient } from "pg"
import { describe, it, expect, beforeAll, afterAll } from "vitest"

import type { Hold } from "../../types.js"
import { holdFunds } from "./holdFunds.js"
import { sweepExpiredHolds } from "./sweepHolds.js"
import { randomString } from "../../utils/helpers.js"
import { createAccounts } from "../../../test/utils.js"
import { startPayment } from "../../payments/payments.js"
import { postTransaction } from "../postTransaction/index.js"
import {
  getTestPool,
  closeTestPool,
  testTransaction,
} from "../../../test/db.js"

afterAll(closeTestPool)

describe("Expired hold sweeper", () => {
  let pool: Pool
  let client: PoolClient
  let sourceAccountId: number
  let systemAccountId: number

  beforeAll(async () => {
    pool = getTestPool()
    client = await pool.connect()

    const { a, s } = await createAccounts(client)
    sourceAccountId = a
    systemAccountId = s
  })

  afterAll(async () => client?.release())

  async function fund(amount: bigint) {
    await testTransaction((c) =>
      postTransaction(
        {
          idempotencyKey: randomString(),
          currency: "PKR",
          postings: [
            { accountId: systemAccountId, amount: -amount },
            { accountId: sourceAccountId, amount },
          ],
        },
        c
      )
    )
  }

  /** A hold that expired an hour ago, owned by nobody until a caller says so. */
  async function expiredHold(amount = 100n): Promise<string> {
    await fund(amount)

    const { holdId } = await testTransaction((c) =>
      holdFunds(
        {
          idempotencyKey: randomString(),
          accountId: sourceAccountId,
          amount,
          currency: "PKR",
          expiresAt: new Date(Date.now() - 60 * 60 * 1000),
        },
        c
      )
    )

    return holdId
  }

  /** A payment that owns `holdId` and is sitting in `state`. */
  async function owner(holdId: string, state: string) {
    const payment = await testTransaction((c) =>
      startPayment(
        {
          idempotencyKey: randomString(),
          sourceAccountId,
          destinationReference: `iban:${randomString()}`,
          amount: 100n,
          currency: "PKR",
          targetCurrency: "PKR",
        },
        c
      )
    )

    await client.query(
      `UPDATE payments
          SET hold_id = $2, state = $3,
              next_attempt_at = NOW() + INTERVAL '1 day'
        WHERE payment_id = $1`,
      [payment.payment_id, holdId, state]
    )

    return payment.payment_id
  }

  const statusOf = async (holdId: string): Promise<Hold["status"]> => {
    const { rows } = await client.query<Hold>(
      "SELECT status FROM holds WHERE hold_id = $1",
      [holdId]
    )
    if (!rows[0]) throw new Error(`no hold row for ${holdId}`)
    return rows[0].status
  }

  const posted = async (accountId: number): Promise<bigint> => {
    const { rows } = await client.query<{ posted: bigint }>(
      "SELECT COALESCE(posted, 0) AS posted FROM account_balances WHERE account_id = $1",
      [accountId]
    )
    return rows[0]?.posted ?? 0n
  }

  it("releases an expired hold no payment is still using", async () => {
    const holdId = await expiredHold()
    const before = await posted(sourceAccountId)

    const results = await sweepExpiredHolds(testTransaction)

    // toContainEqual, not toEqual: HELD:{ccy} and the holds table are shared by
    // every file, so the only honest claim is about this hold.
    expect(results).toContainEqual({ holdId, outcome: "released" })
    expect(await statusOf(holdId)).toBe("released")
    expect((await posted(sourceAccountId)) - before).toBe(100n)
  })

  it("leaves an expired hold alone while its payment is still live", async () => {
    const holdId = await expiredHold()
    await owner(holdId, "settling")

    const results = await sweepExpiredHolds(testTransaction)

    // The whole safety argument. HOLD_TTL_MS is a guess about how long a saga
    // takes, and a guess that is wrong once releases money a payment is still
    // trying to send — while the settle it is in the middle of may already have
    // reached the bank.
    expect(results.map((r) => r.holdId)).not.toContain(holdId)
    expect(await statusOf(holdId)).toBe("held")
  })

  it("sweeps the hold of a payment whose compensation gave up", async () => {
    const holdId = await expiredHold()
    await owner(holdId, "failed")

    const results = await sweepExpiredHolds(testTransaction)

    // This is what makes `failed` an acceptable terminal for a compensation at
    // all: abandonTo returns it knowing the money is stranded, and the sweeper
    // is the thing that eventually unstrands it. Without this the orchestrator
    // would have to choose between a hot loop and losing a customer's money.
    expect(results).toContainEqual({ holdId, outcome: "released" })
    expect(await statusOf(holdId)).toBe("released")
  })

  it("reports a hold it has already released rather than moving it twice", async () => {
    const holdId = await expiredHold()

    await sweepExpiredHolds(testTransaction)
    const after = await posted(sourceAccountId)

    // A second sweep cannot pick it up — `status = 'held'` excludes it — so this
    // asserts the layer beneath: releasing it directly under the derived key is
    // a replay, which is what lets the compensator and the sweeper both own the
    // job without agreeing about who goes first.
    const results = await sweepExpiredHolds(testTransaction)
    expect(results.map((r) => r.holdId)).not.toContain(holdId)

    expect(await posted(sourceAccountId)).toBe(after)
    expect(await statusOf(holdId)).toBe("released")
  })
})
