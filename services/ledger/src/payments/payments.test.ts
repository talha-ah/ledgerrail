import { Pool, type PoolClient } from "pg"
import { describe, it, expect, beforeAll, afterAll } from "vitest"

import { stepKey } from "./states.js"
import { randomString } from "../utils/helpers.js"
import { createAccounts } from "../../test/utils.js"
import type { Payment, PaymentStep } from "../types.js"
import { getTestPool, closeTestPool } from "../../test/db.js"
import {
  advance,
  beginStep,
  finishStep,
  leaseUntil,
  startPayment,
  claimPayments,
  unfinishedSteps,
  type StartPaymentInput,
} from "./payments.js"

afterAll(closeTestPool)

describe("Payment saga state", () => {
  let pool: Pool
  let client: PoolClient
  let sourceAccountId: number

  beforeAll(async () => {
    pool = getTestPool()
    client = await pool.connect()

    const { a } = await createAccounts(client)
    sourceAccountId = a
  })

  afterAll(async () => client?.release())

  const start = (
    idempotencyKey = randomString(),
    overrides: Partial<StartPaymentInput> = {}
  ) =>
    startPayment(
      {
        idempotencyKey,
        sourceAccountId,
        destinationReference: `iban:${idempotencyKey}`,
        amount: 100n,
        currency: "PKR",
        targetCurrency: "PKR",
        ...overrides,
      },
      client
    )

  it("starts in 'initiated' and is idempotent on the caller's key", async () => {
    const key = randomString()

    const first = await start(key)
    const second = await start(key)

    expect(first.state).toBe("initiated")
    expect(second.payment_id).toBe(first.payment_id)

    const { rows } = await client.query<{ n: number }>(
      "SELECT COUNT(*)::int AS n FROM payments WHERE idempotency_key = $1",
      [key]
    )
    expect(rows[0]?.n).toBe(1)
  })

  it("advances along the transition table", async () => {
    const payment = await start()

    const held = await advance(payment.payment_id, "initiated", "held", client)
    expect(held.state).toBe("held")

    const quoted = await advance(payment.payment_id, "held", "quoted", client, {
      quoteRate: "1.0000000000",
      quoteExpiresAt: new Date(Date.now() + 60_000),
    })

    expect(quoted.state).toBe("quoted")
    // NUMERIC comes back as a string, on purpose: a rate multiplies money and
    // must never touch a float.
    expect(typeof quoted.quote_rate).toBe("string")
  })

  it("refuses a transition the machine does not have", async () => {
    const payment = await start()

    await expect(
      advance(payment.payment_id, "initiated", "settled", client)
    ).rejects.toThrow("Illegal transition")

    const { rows } = await client.query<Payment>(
      "SELECT state FROM payments WHERE payment_id = $1",
      [payment.payment_id]
    )
    expect(rows[0]?.state).toBe("initiated")
  })

  it("refuses to advance from a state the payment is no longer in", async () => {
    const payment = await start()
    await advance(payment.payment_id, "initiated", "held", client)

    // The compare-and-set: a second worker still believing the payment is
    // `initiated` updates zero rows and is told, rather than silently
    // overwriting a state it never saw.
    await expect(
      advance(payment.payment_id, "initiated", "held", client)
    ).rejects.toThrow("was not in state initiated")
  })

  it("hands two workers different payments", async () => {
    await start()
    await start()

    const a = await pool.connect()
    const b = await pool.connect()

    try {
      await a.query("BEGIN")
      await b.query("BEGIN")

      const first = await claimPayments(a, 1)
      const second = await claimPayments(b, 1)

      expect(first).toHaveLength(1)
      expect(second).toHaveLength(1)

      // SKIP LOCKED, not blocking: b passed over the row a holds instead of
      // waiting for it. Without it these two workers would form a queue.
      expect(second[0]?.payment_id).not.toBe(first[0]?.payment_id)

      await a.query("ROLLBACK")
      await b.query("ROLLBACK")
    } finally {
      await a.query("ROLLBACK").catch(() => {})
      await b.query("ROLLBACK").catch(() => {})
      a.release()
      b.release()
    }
  })

  it("commits the rail's idempotency key before the call is made", async () => {
    const payment = await start()
    await advance(payment.payment_id, "initiated", "held", client)
    await advance(payment.payment_id, "held", "quoted", client)
    await advance(payment.payment_id, "quoted", "settling", client)

    // Everything up to here is what happens *before* the network call. A crash
    // now is the ambiguous case: the bank may or may not have been reached.
    const step = await beginStep(
      payment.payment_id,
      "settle",
      { amount: "100", currency: "PKR" },
      client
    )

    expect(step.status).toBe("started")
    expect(step.finished_at).toBeNull()
    expect(step.idempotency_key).toBe(stepKey(payment.payment_id, "settle"))

    // What a reconciler finds after the crash: the payment mid-settlement, and
    // the exact key to ask the rail about.
    const stranded = await unfinishedSteps(payment.payment_id, client)
    expect(stranded).toHaveLength(1)
    expect(stranded[0]?.idempotency_key).toBe(step.idempotency_key)
  })

  it("gives every attempt of a step the same key", async () => {
    const payment = await start()
    await advance(payment.payment_id, "initiated", "held", client)
    await advance(payment.payment_id, "held", "quoted", client)
    await advance(payment.payment_id, "quoted", "settling", client)

    const first = await beginStep(payment.payment_id, "settle", {}, client)
    await finishStep(
      payment.payment_id,
      "settle",
      first.attempt,
      { status: "failed", error: "timeout" },
      client
    )

    const second = await beginStep(payment.payment_id, "settle", {}, client)

    // The row is per attempt; the key is per step. A retry that invents a new
    // key is a second payment as far as the bank is concerned.
    expect(second.idempotency_key).toBe(first.idempotency_key)
    expect(second.attempt).toBe(2)
  })

  it("refuses to finish an attempt that was never started", async () => {
    const payment = await start()

    await expect(
      finishStep(
        payment.payment_id,
        "settle",
        99,
        {
          status: "succeeded",
          response: {},
        },
        client
      )
    ).rejects.toThrow("No started attempt")
  })

  it("treats terminal states as terminal", async () => {
    const payment = await start()

    // TRANSITIONS.completed is empty, so canTransition refuses before SQL runs.
    // Nothing else holds that true — a stray entry in the table would make a
    // completed payment restartable.
    await expect(
      advance(payment.payment_id, "completed", "held", client)
    ).rejects.toThrow("Illegal transition")
  })

  it("does not claim a payment whose next attempt is in the future", async () => {
    const payment = await start()

    await advance(payment.payment_id, "initiated", "held", client, {
      nextAttemptAt: new Date(Date.now() + 60 * 60 * 1000),
    })

    const a = await pool.connect()
    try {
      await a.query("BEGIN")
      const claimed = await claimPayments(a, 100)
      // Backoff is the only thing standing between a failing step and a hot
      // loop, and next_attempt_at is where it lives.
      expect(claimed.map((p) => p.payment_id)).not.toContain(payment.payment_id)
      await a.query("ROLLBACK")
    } finally {
      // In finally, not after the assertion: anything that throws above would
      // otherwise return a connection to the pool mid-transaction, and the next
      // test to borrow it fails at BEGIN for reasons that have nothing to do
      // with it. One bug then looks like three.
      await a.query("ROLLBACK").catch(() => {})
      a.release()
    }
  })

  it("does not claim a payment that has finished", async () => {
    const payment = await start()

    await advance(payment.payment_id, "initiated", "held", client)
    await advance(payment.payment_id, "held", "compensating", client)
    await advance(payment.payment_id, "compensating", "failed", client)

    const a = await pool.connect()
    try {
      await a.query("BEGIN")
      const claimed = await claimPayments(a, 100)
      // Same predicate as the partial index. If someone "simplifies" the claim
      // query and the two drift apart, this is what says so.
      expect(claimed.map((p) => p.payment_id)).not.toContain(payment.payment_id)
      await a.query("ROLLBACK")
    } finally {
      await a.query("ROLLBACK").catch(() => {})
      a.release()
    }
  })

  it("refuses an account that cannot pay in the payment's currency", async () => {
    const { rows } = await client.query<{ id: number }>(
      `INSERT INTO accounts (account_number, currency, type)
       VALUES ($1, 'USD', 'asset') RETURNING id;`,
      [randomString()]
    )
    const usdAccount = rows[0]!.id

    await expect(
      startPayment(
        {
          idempotencyKey: randomString(),
          sourceAccountId: usdAccount,
          destinationReference: `iban:${randomString()}`,
          amount: 100n,
          currency: "PKR",
          targetCurrency: "PKR",
        },
        client
      )
    ).rejects.toThrow("No PKR account")

    const { rows: created } = await client.query<{ n: number }>(
      "SELECT COUNT(*)::int AS n FROM payments WHERE source_account_id = $1",
      [usdAccount]
    )

    // Refused at the door: no row, no state machine, nothing for a resumer to
    // pick up and retry forever.
    expect(created[0]?.n).toBe(0)
  })

  it("counts attempts only when asked to", async () => {
    const payment = await start()

    const held = await advance(payment.payment_id, "initiated", "held", client)
    expect(held.attempts).toBe(0)

    const quoted = await advance(payment.payment_id, "held", "quoted", client, {
      bumpAttempts: true,
      nextAttemptAt: leaseUntil(),
    })

    expect(quoted.attempts).toBe(1)
    expect(quoted.next_attempt_at.getTime()).toBeGreaterThan(Date.now())
  })

  it("refuses a key reused with different parameters", async () => {
    const key = randomString()
    const original = await start(key)

    await expect(start(key, { amount: 500n })).rejects.toThrow(
      "different parameters"
    )

    // The original is untouched — a mismatched replay is refused, not applied.
    const { rows } = await client.query<Payment>(
      "SELECT * FROM payments WHERE idempotency_key = $1",
      [key]
    )
    expect(rows).toHaveLength(1)
    expect(rows[0]?.amount).toBe(original.amount)
  })
})
