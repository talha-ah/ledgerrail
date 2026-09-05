import { Pool, type PoolClient } from "pg"
import { describe, it, expect, beforeAll, afterAll } from "vitest"

import { stepKey } from "./states.js"
import { randomString } from "../utils/helpers.js"
import { PermanentError } from "../utils/error.js"
import { createAccounts } from "../../test/utils.js"
import type { Hold, PaymentStep } from "../types.js"
import { createFakeRail, type Rail } from "../rails/index.js"
import { postTransaction } from "../commands/postTransaction/index.js"
import { getTestPool, closeTestPool, testTransaction } from "../../test/db.js"
import { tick, type OrchestratorDeps, type TickResult } from "./orchestrator.js"
import {
  advance,
  takeLease,
  getPayment,
  startPayment,
  MAX_ATTEMPTS,
} from "./payments.js"

afterAll(closeTestPool)

describe("Payment orchestrator", () => {
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

  const deps = (rail: Rail): OrchestratorDeps => ({
    rail,
    transaction: testTransaction,
  })

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

  async function begin(amount = 1_000n) {
    await fund(amount)

    return testTransaction((c) =>
      startPayment(
        {
          idempotencyKey: randomString(),
          sourceAccountId,
          destinationReference: `iban:${randomString()}`,
          amount,
          currency: "PKR",
          targetCurrency: "PKR",
        },
        c
      )
    )
  }

  const read = (id: string) => testTransaction((c) => getPayment(id, c))

  /** Backoff is real, so a test that wants the next attempt must ask for it. */
  const makeClaimable = (id: string) =>
    client.query(
      "UPDATE payments SET next_attempt_at = NOW() WHERE payment_id = $1",
      [id]
    )

  /**
   * HELD:{ccy} and RAIL:{ccy} are seeded once by a migration and shared by every
   * test in the database — `createAccounts` isolates a file's own accounts and
   * cannot isolate these. So the only honest claim a test can make about them is
   * what it moved, not what they hold.
   */
  const systemBalance = async (accountNumber: string): Promise<bigint> => {
    const { rows } = await client.query<{ posted: bigint }>(
      `SELECT COALESCE(b.posted, 0) AS posted
         FROM accounts a
         LEFT JOIN account_balances b ON b.account_id = a.id
        WHERE a.account_number = $1`,
      [accountNumber]
    )

    return rows[0]?.posted ?? 0n
  }

  async function driveToEnd(d: OrchestratorDeps, id: string, maxTicks = 12) {
    const seen: TickResult[] = []

    for (let i = 0; i < maxTicks; i++) {
      const payment = await read(id)
      if (!payment) throw new Error("payment vanished")
      if (
        ["completed", "compensated", "failed", "compensating"].includes(
          payment.state
        )
      )
        break

      await makeClaimable(id)
      seen.push(...(await tick(d)).filter((r) => r.payment === id))
    }

    return seen
  }

  it("drives a payment from initiated to completed", async () => {
    const payment = await begin()
    const d = deps(createFakeRail())

    const railBefore = await systemBalance("RAIL:PKR")
    const heldBefore = await systemBalance("HELD:PKR")

    await driveToEnd(d, payment.payment_id)

    const final = await read(payment.payment_id)
    expect(final?.state).toBe("completed")
    expect(final?.hold_id).not.toBeNull()
    expect(final?.quote_id).not.toBeNull()
    expect(final?.attempts).toBe(0)

    // The hold was consumed, not abandoned.
    const { rows: holds } = await client.query<Hold>(
      "SELECT * FROM holds WHERE hold_id = $1",
      [final!.hold_id]
    )
    expect(holds[0]?.status).toBe("captured")

    // The invariant that outlives every checkpoint. ::BIGINT because SUM of a
    // BIGINT is NUMERIC, which arrives as a string while the column does not.
    const { rows: total } = await client.query<{ sum: bigint }>(
      "SELECT COALESCE(SUM(posted), 0)::BIGINT AS sum FROM account_balances"
    )
    expect(total[0]?.sum).toBe(0n)

    // Deltas, because these accounts are shared. The money left the customer,
    // passed through the reserve, and landed in the clearing account that stands
    // for "handed to the rail" — a net zero through HELD and a net 1,000 into
    // RAIL, whatever else the database happens to be holding.
    expect((await systemBalance("RAIL:PKR")) - railBefore).toBe(1_000n)
    expect((await systemBalance("HELD:PKR")) - heldBefore).toBe(0n)
  })

  it("advances one step per tick", async () => {
    const payment = await begin()
    const d = deps(createFakeRail())

    const states = []
    for (let i = 0; i < 4; i++) {
      await makeClaimable(payment.payment_id)
      await tick(d)
      states.push((await read(payment.payment_id))!.state)
    }

    // `settling` never appears: it is the middle of the settle step, not a
    // resting place, so one tick carries the payment through it.
    expect(states).toEqual(["held", "quoted", "settled", "completed"])
  })

  it("resumes a payment abandoned inside the settle window without paying twice", async () => {
    const rail = createFakeRail()
    const payment = await begin()
    const d = deps(rail)

    await makeClaimable(payment.payment_id)
    await tick(d) // → held
    await makeClaimable(payment.payment_id)
    await tick(d) // → quoted

    const quoted = (await read(payment.payment_id))!
    const key = stepKey(payment.payment_id, "settle")

    // The rail settles, and the answer never gets home — a worker that died
    // between the call and the commit.
    const original = await rail.settle({
      idempotencyKey: key,
      quoteId: quoted.quote_id!,
      destinationReference: quoted.destination_reference,
      amount: String(quoted.amount),
      currency: quoted.currency,
      targetCurrency: quoted.target_currency,
    })

    await client.query(
      "UPDATE payments SET state = 'settling', next_attempt_at = NOW() WHERE payment_id = $1",
      [payment.payment_id]
    )

    await driveToEnd(d, payment.payment_id)

    expect((await read(payment.payment_id))?.state).toBe("completed")

    // Same key, same settlement. The retry cost the customer nothing because the
    // key was committed before the call and never regenerated.
    expect(await rail.lookup(key)).toEqual(original)

    const { rows: steps } = await client.query<PaymentStep>(
      "SELECT * FROM payment_steps WHERE payment_id = $1 AND step = 'settle'",
      [payment.payment_id]
    )
    expect(steps.every((s) => s.idempotency_key === key)).toBe(true)
  })

  it("holds its state and backs off when the rail fails transiently", async () => {
    const payment = await begin()
    const d = deps(
      createFakeRail({
        beforeSettle: () => {
          throw new Error("ECONNRESET")
        },
      })
    )

    await makeClaimable(payment.payment_id)
    await tick(d)
    await makeClaimable(payment.payment_id)
    await tick(d)

    const before = (await read(payment.payment_id))!
    await makeClaimable(payment.payment_id)
    const [result] = (await tick(d)).filter(
      (r) => r.payment === payment.payment_id
    )

    expect(result?.outcome).toBe("retry")

    const after = (await read(payment.payment_id))!
    // Still in the ambiguous window, because we do not know it never landed.
    expect(after.state).toBe("settling")
    expect(after.attempts).toBeGreaterThan(before.attempts)
    expect(after.next_attempt_at.getTime()).toBeGreaterThan(Date.now())
    expect(after.last_error).toContain("ECONNRESET")
  })

  it("compensates rather than failing once a hold exists", async () => {
    const payment = await begin()
    const d = deps(
      createFakeRail({
        beforeSettle: () => {
          throw new PermanentError("account closed")
        },
      })
    )

    await driveToEnd(d, payment.payment_id)

    const final = (await read(payment.payment_id))!
    // Not `failed`: the customer's money is in the reserve, and only a
    // compensation gives it back. 3.4 drives it from here.
    expect(final.state).toBe("compensating")
    expect(final.last_error).toContain("account closed")

    const { rows } = await client.query<Hold>(
      "SELECT * FROM holds WHERE hold_id = $1",
      [final.hold_id]
    )
    expect(rows[0]?.status).toBe("held")
  })

  it("gives up after MAX_ATTEMPTS of a transient failure", async () => {
    const payment = await begin()
    const d = deps(
      createFakeRail({
        beforeSettle: () => {
          throw new Error("ETIMEDOUT")
        },
      })
    )

    await makeClaimable(payment.payment_id)
    await tick(d)
    await makeClaimable(payment.payment_id)
    await tick(d)

    await client.query(
      "UPDATE payments SET attempts = $2, next_attempt_at = NOW() WHERE payment_id = $1",
      [payment.payment_id, MAX_ATTEMPTS - 1]
    )

    const [result] = (await tick(d)).filter(
      (r) => r.payment === payment.payment_id
    )

    expect(result?.outcome).toBe("abandoned")
    expect((await read(payment.payment_id))?.state).toBe("compensating")
  })

  it("re-prices a quote that has actually expired", async () => {
    const payment = await begin()
    const d: OrchestratorDeps = {
      ...deps(createFakeRail({ quoteTtlMs: 500 })),
      quoteMarginMs: 50,
    }

    await makeClaimable(payment.payment_id)
    await tick(d)
    await makeClaimable(payment.payment_id)
    await tick(d)

    const quoted = (await read(payment.payment_id))!
    expect(quoted.state).toBe("quoted")

    // Real time, because the expiry is a fact shared with the rail. Ageing only
    // our copy asks for a divergence that cannot happen — `quote_expires_at` is
    // never written from anywhere but a QuoteResponse.
    await new Promise((r) => setTimeout(r, 600))

    await makeClaimable(payment.payment_id)
    await tick(d)
    expect((await read(payment.payment_id))!.state).toBe("held")

    await driveToEnd(d, payment.payment_id)

    const final = (await read(payment.payment_id))!
    expect(final.state).toBe("completed")
    expect(final.quote_id).not.toBe(quoted.quote_id)

    // The call landed and its answer was recorded, even though the step failed.
    // A row left `started` is what 3.5's reconciler treats as "never learned".
    const { rows } = await client.query<PaymentStep>(
      "SELECT * FROM payment_steps WHERE payment_id = $1 AND step = 'quote' ORDER BY attempt",
      [payment.payment_id]
    )
    expect(rows.every((s) => s.status !== "started")).toBe(true)
  })

  it("parks a state it cannot drive instead of spinning on it", async () => {
    const payment = await begin()
    const d = deps(createFakeRail())

    await makeClaimable(payment.payment_id)
    await tick(d)

    await client.query(
      "UPDATE payments SET state = 'compensating', next_attempt_at = NOW() WHERE payment_id = $1",
      [payment.payment_id]
    )

    const [result] = (await tick(d)).filter(
      (r) => r.payment === payment.payment_id
    )

    expect(result?.outcome).toBe("parked")

    // `compensating` is not terminal, so claimPayments will keep offering it
    // until 3.4 exists. Without this the orchestrator is a hot loop against its
    // own database, and the default branch is the only thing preventing it.
    const parked = (await read(payment.payment_id))!
    expect(parked.next_attempt_at.getTime()).toBeGreaterThan(
      Date.now() + 30 * 60 * 1000
    )
  })

  it("passes over a payment another worker holds", async () => {
    const payment = await begin()
    const d = deps(createFakeRail())

    const other = await pool.connect()

    try {
      await other.query("BEGIN")
      await other.query(
        "SELECT * FROM payments WHERE payment_id = $1 FOR UPDATE",
        [payment.payment_id]
      )

      // SKIP LOCKED, not blocking: the tick passes over the held row instead of
      // queueing behind it. Racing two ticks could never prove this — they are
      // free to take the payment in sequence and both legitimately succeed.
      const during = await tick(d)
      expect(during.map((r) => r.payment)).not.toContain(payment.payment_id)

      await other.query("ROLLBACK")
    } finally {
      await other.query("ROLLBACK").catch(() => {})
      other.release()
    }

    const after = await tick(d)
    expect(after.map((r) => r.payment)).toContain(payment.payment_id)
  })

  it("does not spin when a quote is live but too close to expiry", async () => {
    const payment = await begin()
    const d = deps(createFakeRail())

    await makeClaimable(payment.payment_id)
    await tick(d)
    await makeClaimable(payment.payment_id)
    await tick(d)

    const before = (await read(payment.payment_id))!

    // Inside the margin but NOT yet expired — the one window where the
    // orchestrator calls a quote dead and the rail still replays it.
    await client.query(
      `UPDATE payments
          SET quote_expires_at = NOW() + INTERVAL '2 seconds', next_attempt_at = NOW()
        WHERE payment_id = $1`,
      [payment.payment_id]
    )

    await tick(d)

    const parked = (await read(payment.payment_id))!
    expect(parked.state).toBe("held")

    // Two things stop the spin, and neither is `attempts`. The wait stops it
    // happening: re-asking now would be handed the same quote back, so the
    // payment sleeps until that quote is genuinely dead. The quote-step count
    // stops it lasting. `attempts` cannot do either job, because the successful
    // re-quote that ends each lap resets it — correctly, since a fresh quote
    // earns a fresh step budget.
    expect(parked.next_attempt_at.getTime()).toBeGreaterThan(Date.now())
    expect(parked.attempts).toBe(before.attempts)

    // The call landed and its answer was recorded, even though the step failed.
    // A row left `started` is what 3.5's reconciler treats as "never learned".
    const { rows } = await client.query<PaymentStep>(
      "SELECT * FROM payment_steps WHERE payment_id = $1 AND step = 'quote' ORDER BY attempt",
      [payment.payment_id]
    )
    expect(rows.every((s) => s.status !== "started")).toBe(true)
  })

  it("refuses a stale worker's abandonment", async () => {
    const payment = await begin()
    const d = deps(createFakeRail())

    await makeClaimable(payment.payment_id)
    await tick(d) // → held

    const stale = (await read(payment.payment_id))!

    // A second worker takes the payment: same state, new token.
    const reclaimed = await testTransaction((c) => takeLease(stale, c))
    expect(reclaimed!.lease_token).not.toBe(stale.lease_token)

    // The first worker, still holding the old token, tries to give up on it.
    const result = await (async () => {
      try {
        await testTransaction((c) =>
          advance(stale.payment_id, "held", "compensating", c, {
            lastError: "stale",
            leaseToken: stale.lease_token,
          })
        )
        return "applied"
      } catch {
        return "refused"
      }
    })()

    expect(result).toBe("refused")

    // The winner's payment is untouched — not compensating, not carrying
    // someone else's error.
    const after = (await read(payment.payment_id))!
    expect(after.state).toBe("held")
    expect(after.last_error).not.toBe("stale")
  })
})
