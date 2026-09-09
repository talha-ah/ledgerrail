import { Pool, type PoolClient } from "pg"
import { describe, it, expect, beforeAll, afterAll } from "vitest"

import { stepKey } from "./states.js"
import type { Currency } from "@ledgerrail/money"
import { randomString } from "../utils/helpers.js"
import { PermanentError } from "../utils/error.js"
import { createAccounts } from "../../test/utils.js"
import { createFakeRail, type Rail } from "../rails/index.js"
import type { Hold, PaymentStep, PaymentStepName } from "../types.js"
import { postTransaction } from "../commands/postTransaction/index.js"
import type { ReverseRequest, ReverseResponse } from "../rails/index.js"
import { getTestPool, closeTestPool, testTransaction } from "../../test/db.js"
import { tick, type OrchestratorDeps, type TickResult } from "./orchestrator.js"
import {
  advance,
  takeLease,
  listSteps,
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

  async function begin(amount = 1_000n, targetCurrency: Currency = "PKR") {
    await fund(amount)

    return testTransaction((c) =>
      startPayment(
        {
          idempotencyKey: randomString(),
          sourceAccountId,
          destinationReference: `iban:${randomString()}`,
          amount,
          currency: "PKR",
          targetCurrency,
        },
        c
      )
    )
  }

  const read = (id: string) => testTransaction((c) => getPayment(id, c))

  /** Backoff is real, so a test that wants the next attempt must ask for it. */
  const makeClaimable = async (id: string) => {
    await client.query(
      `UPDATE payments
          SET next_attempt_at = NOW() + INTERVAL '1 day'
        WHERE payment_id <> $1
          AND state NOT IN ('completed', 'compensated', 'failed')`,
      [id]
    )

    await client.query(
      "UPDATE payments SET next_attempt_at = NOW() WHERE payment_id = $1",
      [id]
    )
  }

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

  const posted = async (accountId: number): Promise<bigint> => {
    const { rows } = await client.query<{ posted: bigint }>(
      "SELECT COALESCE(posted, 0) AS posted FROM account_balances WHERE account_id = $1",
      [accountId]
    )
    return rows[0]?.posted ?? 0n
  }

  const holdOf = async (holdId: string): Promise<Hold> => {
    const { rows } = await client.query<Hold>(
      "SELECT * FROM holds WHERE hold_id = $1",
      [holdId]
    )
    if (!rows[0]) throw new Error(`no hold row for ${holdId}`)
    return rows[0]
  }

  const stepsOf = (id: string, step: PaymentStepName): Promise<PaymentStep[]> =>
    testTransaction((c) => listSteps(id, step, c))

  const expire = (holdId: string) =>
    client.query(
      "UPDATE holds SET expires_at = NOW() - INTERVAL '1 hour' WHERE hold_id = $1",
      [holdId]
    )

  /** initiated → held → quoted → settled. One step per tick, so three ticks. */
  async function driveToSettled(d: OrchestratorDeps, id: string) {
    for (let i = 0; i < 3; i++) {
      await makeClaimable(id)
      await tick(d)
    }
  }

  async function driveToEnd(d: OrchestratorDeps, id: string, maxTicks = 12) {
    const seen: TickResult[] = []

    for (let i = 0; i < maxTicks; i++) {
      const payment = await read(id)
      if (!payment) throw new Error("payment vanished")
      if (["completed", "compensated", "failed"].includes(payment.state)) break

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

    // Was `compensating` — the furthest 3.3 could take it. The reason it is not
    // `failed` is unchanged: the customer's money was in the reserve, and only a
    // compensation gives it back. 3.4 is that compensation.
    expect(final.state).toBe("compensated")
    expect(final.last_error).toContain("account closed")

    const { rows } = await client.query<Hold>(
      "SELECT * FROM holds WHERE hold_id = $1",
      [final.hold_id]
    )
    expect(rows[0]?.status).toBe("released")
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

  it("drives a compensating payment instead of parking it", async () => {
    const payment = await begin()
    const d = deps(createFakeRail())

    await makeClaimable(payment.payment_id)
    await tick(d) // → held, so there is something to give back

    await client.query(
      "UPDATE payments SET state = 'compensating', next_attempt_at = NOW() WHERE payment_id = $1",
      [payment.payment_id]
    )

    const [result] = (await tick(d)).filter(
      (r) => r.payment === payment.payment_id
    )

    // Until 3.4 this came back `parked`, an hour out: claimPayments keeps
    // offering a non-terminal state, and an orchestrator with no handler for one
    // is a hot loop against its own database.
    expect(result).toEqual({
      payment: payment.payment_id,
      outcome: "advanced",
      state: "compensated",
    })

    const final = (await read(payment.payment_id))!
    expect((await holdOf(final.hold_id!)).status).toBe("released")
  })

  it("releases the hold and compensates when nothing reached the rail", async () => {
    // One PKR minor unit is worth less than a US cent, so the quote is refused
    // permanently. A real failure, and one that happens before the rail is ever
    // asked to move anything.
    const payment = await begin(1n, "USD")
    const d = deps(createFakeRail())

    const heldBefore = await systemBalance("HELD:PKR")
    const sourceBefore = await posted(sourceAccountId)

    await driveToEnd(d, payment.payment_id)

    const final = (await read(payment.payment_id))!
    expect(final.state).toBe("compensated")

    // Carried, not cleared. `advance` wipes last_error on a successful move,
    // which is right everywhere except here: why this payment did not go through
    // is the only thing anyone will ever ask a compensated row.
    expect(final.last_error).toContain("rounds to nothing")

    // The rail was never asked to send anything back, because nothing was sent.
    // The absence of a settle step is what says so — `payments` no longer
    // remembers which state this was abandoned from, and should not have to.
    expect(await stepsOf(payment.payment_id, "settle")).toHaveLength(0)
    expect(await stepsOf(payment.payment_id, "reverse")).toHaveLength(0)

    expect((await holdOf(final.hold_id!)).status).toBe("released")
    expect((await systemBalance("HELD:PKR")) - heldBefore).toBe(0n)
    expect((await posted(sourceAccountId)) - sourceBefore).toBe(0n)
  })

  it("reverses at the rail before releasing a hold whose settle may have landed", async () => {
    const rail = createFakeRail()
    const payment = await begin()
    const d = deps(rail)

    const heldBefore = await systemBalance("HELD:PKR")
    const railBefore = await systemBalance("RAIL:PKR")
    const sourceBefore = await posted(sourceAccountId)

    await driveToSettled(d, payment.payment_id)
    const settled = (await read(payment.payment_id))!
    expect(settled.state).toBe("settled")

    // A hold is meant to outlive its saga — until it does not. An expired hold
    // refuses capture, which is the honest route from `settled` into
    // `compensating`: the rail has the money and the reserve can no longer send
    // it on. No SQL surgery on `state` to get here.
    await expire(settled.hold_id!)

    await driveToEnd(d, payment.payment_id)

    const final = (await read(payment.payment_id))!
    expect(final.state).toBe("compensated")
    expect(final.last_error).toContain("Hold has expired")

    const reverses = await stepsOf(payment.payment_id, "reverse")
    expect(reverses).toHaveLength(1)
    expect(reverses[0]?.status).toBe("succeeded")
    expect((reverses[0]?.response as ReverseResponse).status).toBe("reversed")

    // Named by the key the settle was sent with, not by the reference it came
    // back with. The reference is the version that does not work — a settle
    // whose answer was lost has none, and that is the settlement this exists for.
    expect((reverses[0]?.request as ReverseRequest).settleIdempotencyKey).toBe(
      stepKey(payment.payment_id, "settle")
    )

    // And the rail agrees it no longer holds the money — which is what 3.5 will
    // ask it, through the same map.
    const atRail = await rail.lookup(stepKey(payment.payment_id, "settle"))
    expect(atRail?.status).toBe("reversed")

    // Capture never ran, so nothing ever reached the clearing account.
    expect((await systemBalance("RAIL:PKR")) - railBefore).toBe(0n)
    expect((await systemBalance("HELD:PKR")) - heldBefore).toBe(0n)
    expect((await posted(sourceAccountId)) - sourceBefore).toBe(0n)
  })

  it("reverses a settlement whose answer was never received", async () => {
    const payment = await begin()
    let asked: ReverseRequest | null = null

    const d = deps(
      createFakeRail({
        // Transient: the call may have reached the bank and only the answer was
        // lost. callRail leaves the step `started`, and that is the whole reason
        // the rule is "not failed" rather than "succeeded" — "we do not know"
        // has to compensate exactly like "we do".
        beforeSettle: () => {
          throw new Error("ETIMEDOUT")
        },
        beforeReverse: (req) => {
          asked = req
        },
      })
    )

    await driveToSettled(d, payment.payment_id) // gets as far as `settling`
    expect((await read(payment.payment_id))?.state).toBe("settling")

    // Spend the budget so the next failure abandons instead of backing off.
    await client.query(
      "UPDATE payments SET attempts = $2, next_attempt_at = NOW() WHERE payment_id = $1",
      [payment.payment_id, MAX_ATTEMPTS - 1]
    )

    await driveToEnd(d, payment.payment_id)

    const final = (await read(payment.payment_id))!
    expect(final.state).toBe("compensated")

    const settles = await stepsOf(payment.payment_id, "settle")
    expect(settles.some((s) => s.status === "started")).toBe(true)

    // Asked anyway. The fake holds no settlement under that key, so it answers
    // `not_found` — an answer, not an error, and therefore a succeeded step: we
    // asked, and learned there was nothing there.
    expect(asked).not.toBeNull()
    const reverses = await stepsOf(payment.payment_id, "reverse")
    expect(reverses).toHaveLength(1)
    expect(reverses[0]?.status).toBe("succeeded")
    expect((reverses[0]?.response as ReverseResponse).status).toBe("not_found")

    expect((await holdOf(final.hold_id!)).status).toBe("released")
  })

  it("does not ask the rail to reverse a settlement the rail refused", async () => {
    const payment = await begin()
    let reverses = 0

    const d = deps(
      createFakeRail({
        beforeSettle: () => {
          throw new PermanentError("account closed")
        },
        beforeReverse: () => {
          reverses += 1
        },
      })
    )

    await driveToEnd(d, payment.payment_id)

    const final = (await read(payment.payment_id))!
    expect(final.state).toBe("compensated")

    // `failed` is the rail's own "no" — there is nothing at the far end to send
    // back, and asking would be a reversal of a payment that never happened.
    // The test above is the same code path with a `started` row instead.
    const settles = await stepsOf(payment.payment_id, "settle")
    expect(settles.length).toBeGreaterThan(0)
    expect(settles.every((s) => s.status === "failed")).toBe(true)

    expect(reverses).toBe(0)
    expect(await stepsOf(payment.payment_id, "reverse")).toHaveLength(0)
    expect((await holdOf(final.hold_id!)).status).toBe("released")
  })

  it("presents the same reverse key on every attempt", async () => {
    const payment = await begin()
    const keys: string[] = []

    const d = deps(
      createFakeRail({
        beforeReverse: (req) => {
          keys.push(req.idempotencyKey)
          if (keys.length === 1) throw new Error("ECONNRESET")
        },
      })
    )

    const heldBefore = await systemBalance("HELD:PKR")

    await driveToSettled(d, payment.payment_id)
    const settled = (await read(payment.payment_id))!
    await expire(settled.hold_id!)

    await driveToEnd(d, payment.payment_id)

    const final = (await read(payment.payment_id))!
    expect(final.state).toBe("compensated")

    // Two attempts, one key. A retry that invents a new one is a second reversal
    // as far as the bank is concerned — the rule that keeps stepKey per step
    // rather than per attempt, applied to the compensation.
    expect(keys).toHaveLength(2)
    expect(new Set(keys).size).toBe(1)
    expect(keys[0]).toBe(stepKey(payment.payment_id, "reverse"))

    const reverses = await stepsOf(payment.payment_id, "reverse")
    expect(reverses).toHaveLength(2)
    expect(reverses[0]?.status).toBe("started") // never learned
    expect(reverses[1]?.status).toBe("succeeded")

    // And the money came back exactly once, because the release key is derived
    // from the hold and the journal refuses the second one.
    expect((await systemBalance("HELD:PKR")) - heldBefore).toBe(0n)
    const { rowCount } = await client.query(
      "SELECT 1 FROM events WHERE idempotency_key = $1",
      [`hold:${final.hold_id}:release`]
    )
    expect(rowCount).toBe(1)
  })

  it("fails a compensation it cannot finish instead of retrying it forever", async () => {
    const payment = await begin()
    const d = deps(
      createFakeRail({
        beforeReverse: () => {
          throw new PermanentError("reversal window closed")
        },
      })
    )

    await driveToSettled(d, payment.payment_id)
    const settled = (await read(payment.payment_id))!
    await expire(settled.hold_id!)

    await driveToEnd(d, payment.payment_id)

    const final = (await read(payment.payment_id))!

    // Before 3.4, abandonTo("compensating") returned null and this rescheduled
    // forever with a clean record and a budget it never spent — patience that
    // was really a hot loop. There is nowhere to compensate *to*.
    expect(final.state).toBe("failed")
    expect(final.last_error).toContain("reversal window closed")

    // The money is stranded in the reserve, and `failed` is only an acceptable
    // terminal because the sweeper is behind it: this hold is expired and its
    // payment is now terminal, which is exactly what claimExpired selects.
    // Proven in sweepHolds.test.ts — "sweeps the hold of a payment whose
    // compensation gave up" — rather than here, so this file never runs a
    // sweep that could release another test's hold.
    expect((await holdOf(final.hold_id!)).status).toBe("held")
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
