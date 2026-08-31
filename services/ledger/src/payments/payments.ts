import type { PoolClient } from "pg"
import { createHash, randomUUID } from "node:crypto"

import type { Currency } from "@ledgerrail/money"
import { canTransition, stepKey } from "./states.js"
import type {
  Payment,
  PaymentStep,
  PaymentState,
  PaymentStepName,
} from "../types.js"

export interface StartPaymentInput {
  idempotencyKey: string
  sourceAccountId: number
  destinationReference: string
  amount: bigint
  currency: Currency
  targetCurrency: Currency
}

/**
 * How long a worker may hold a payment before another may take it.
 *
 * The row lock is held only long enough to *record* the claim — the rail call
 * happens after the commit, because network I/O inside a transaction holds locks
 * across a round trip you do not control. So the durable claim is the state plus
 * next_attempt_at: moving a payment into a working state pushes its lease
 * forward, and a worker that dies mid-call leaves it reclaimable when the lease
 * expires rather than stranded forever.
 */
export const LEASE_MS = 30_000
export const leaseUntil = (ms = LEASE_MS) => new Date(Date.now() + ms)

/**
 * What the caller asked for, independent of the key they asked with.
 *
 * An ordered array joined by NULL rather than JSON.stringify: object key order
 * would leak into the hash, and JSON.stringify throws on a bigint anyway — the
 * amount has to be stringified deliberately, as it is everywhere else here.
 *
 * The idempotency key is not part of it. The key is the question; this is the
 * request being asked about.
 */
function fingerprintOf(input: StartPaymentInput): string {
  return createHash("sha256")
    .update(
      [
        input.sourceAccountId,
        input.destinationReference,
        String(input.amount),
        input.currency,
        input.targetCurrency,
      ].join("\u0000")
    )
    .digest("hex")
}

export async function startPayment(
  input: StartPaymentInput,
  client: PoolClient
): Promise<Payment> {
  const fingerprint = fingerprintOf(input)

  const { rows } = await client.query<Payment>(
    `
    INSERT INTO payments
      (payment_id, idempotency_key, state, source_account_id,
       destination_reference, amount, currency, target_currency, request_fingerprint)
    -- INSERT … SELECT rather than VALUES: the account must exist *and* be
    -- denominated in the currency being paid from. A saga that cannot possibly
    -- succeed should not get a row and a state machine to walk through first.
    SELECT $1, $2, 'initiated', a.id, $4, $5, $6, $7, $8
      FROM accounts a
     WHERE a.id = $3 AND a.currency = $6
    ON CONFLICT (idempotency_key) DO NOTHING
    RETURNING *;
    `,
    [
      randomUUID(),
      input.idempotencyKey,
      input.sourceAccountId,
      input.destinationReference,
      input.amount,
      input.currency,
      input.targetCurrency,
      fingerprint,
    ]
  )

  if (rows[0]) return rows[0]

  // Zero rows now means one of two things, and they need different answers.
  const { rows: existing } = await client.query<Payment>(
    "SELECT * FROM payments WHERE idempotency_key = $1",
    [input.idempotencyKey]
  )

  const original = existing[0]
  if (!original) {
    // No original either, so the SELECT matched no account.
    throw new Error(
      `No ${input.currency} account ${input.sourceAccountId} to pay from`
    )
  }

  // A NULL fingerprint predates this column and cannot be compared — the
  // honest answer for a forward-only migration is to let it through rather
  // than reject a payment on evidence that was never recorded.
  if (original.request_fingerprint !== fingerprint) {
    throw new Error(
      `Idempotency key ${input.idempotencyKey} was used with different parameters`
    )
  }

  return original
}

export interface AdvanceFields {
  holdId?: string
  quoteRate?: string
  quoteExpiresAt?: Date
  nextAttemptAt?: Date
  lastError?: string | null
  /** Count this transition as an attempt at a side effect, not just a move. */
  bumpAttempts?: boolean
}

/**
 * Compare-and-set. The `from` state in the WHERE clause is what stops two
 * orchestrators driving one payment past each other: the loser updates zero
 * rows and is told so, rather than silently overwriting a state it never saw.
 */
export async function advance(
  paymentId: string,
  from: PaymentState,
  to: PaymentState,
  client: PoolClient,
  fields: AdvanceFields = {}
): Promise<Payment> {
  // Checked here as well as in SQL, and the two catch different things: this
  // catches a programmer describing a transition the machine does not have,
  // the WHERE clause catches a racing writer.
  if (!canTransition(from, to)) {
    throw new Error(`Illegal transition: ${from} → ${to}`)
  }

  const { rows } = await client.query<Payment>(
    `
    UPDATE payments
    SET state            = $3,
        -- COALESCE rather than dynamic SQL: an omitted field is left alone, and
        -- the statement stays one fixed string with bound parameters.
        hold_id          = COALESCE($4, hold_id),
        quote_rate       = COALESCE($5, quote_rate),
        quote_expires_at = COALESCE($6, quote_expires_at),
        next_attempt_at  = COALESCE($7, next_attempt_at),
        -- Not COALESCEd, unlike the fields above: a successful transition means
        -- the previous failure is history, so an omitted lastError clears it.
        last_error       = $8,
        -- Explicit rather than automatic: "how many times has this been tried"
        -- and "how many times has this moved" are different questions, and they
        -- stop agreeing the moment compensation starts moving it too.
        attempts         = attempts + CASE WHEN $9::boolean THEN 1 ELSE 0 END,
        updated_at       = NOW()
    WHERE payment_id = $1 AND state = $2
    RETURNING *;
    `,
    [
      paymentId,
      from,
      to,
      fields.holdId ?? null,
      fields.quoteRate ?? null,
      fields.quoteExpiresAt ?? null,
      fields.nextAttemptAt ?? null,
      fields.lastError ?? null,
      fields.bumpAttempts ?? false,
    ]
  )

  if (!rows[0]) {
    throw new Error(`Payment ${paymentId} was not in state ${from}`)
  }

  return rows[0]
}

/**
 * Claims work for this worker. SKIP LOCKED is what makes two orchestrators
 * cooperate rather than queue: a row another transaction already holds is passed
 * over instead of blocking on it.
 *
 * The caller must own a transaction, but only long enough to advance each claimed
 * payment into a working state with a fresh lease — then commit, then act. Holding
 * the transaction across the rail call would put a network round trip inside a
 * lock, which is the outage this whole design exists to avoid.
 */
export async function claimPayments(
  client: PoolClient,
  limit = 10
): Promise<Payment[]> {
  const { rows } = await client.query<Payment>(
    `
    SELECT * FROM payments
    WHERE state NOT IN ('completed', 'compensated', 'failed')
      AND next_attempt_at <= NOW()
    ORDER BY next_attempt_at
    LIMIT $1
    FOR UPDATE SKIP LOCKED;
    `,
    [limit]
  )

  return rows
}

/**
 * Records the intent to call a rail, and is committed BEFORE the call happens.
 * This is the whole crash-safety rule: after this commits, a crash leaves a row
 * saying "we were about to do X with key K" — which is exactly what a reconciler
 * needs to ask the rail what it thinks happened.
 */
export async function beginStep(
  paymentId: string,
  step: PaymentStepName,
  request: unknown,
  client: PoolClient
): Promise<PaymentStep> {
  const { rows } = await client.query<PaymentStep>(
    `
    INSERT INTO payment_steps
      (payment_id, step, attempt, idempotency_key, status, request)
    SELECT $1, $2, COALESCE(MAX(attempt), 0) + 1, $3, 'started', $4
      FROM payment_steps
     WHERE payment_id = $1 AND step = $2
    RETURNING *;
    `,
    [
      paymentId,
      step,
      stepKey(paymentId, step),
      request === undefined ? null : JSON.stringify(request),
    ]
  )

  if (!rows[0]) throw new Error("failed to record step intent")
  return rows[0]
}

export async function finishStep(
  paymentId: string,
  step: PaymentStepName,
  attempt: number,
  outcome:
    | { status: "succeeded"; response: unknown }
    | { status: "failed"; error: string },
  client: PoolClient
): Promise<void> {
  const { rowCount } = await client.query(
    `
    UPDATE payment_steps
    SET status      = $4,
        response    = $5,
        error       = $6,
        finished_at = NOW()
    WHERE payment_id = $1 AND step = $2 AND attempt = $3 AND status = 'started'
    `,
    [
      paymentId,
      step,
      attempt,
      outcome.status,
      outcome.status === "succeeded" ? JSON.stringify(outcome.response) : null,
      outcome.status === "failed" ? outcome.error : null,
    ]
  )

  if (rowCount !== 1) {
    throw new Error(`No started attempt ${attempt} of ${step} to finish`)
  }
}

/** The attempts whose result was never learned — 3.5's reconciler reads these. */
export async function unfinishedSteps(
  paymentId: string,
  client: PoolClient
): Promise<PaymentStep[]> {
  const { rows } = await client.query<PaymentStep>(
    `SELECT * FROM payment_steps
      WHERE payment_id = $1 AND status = 'started'
      ORDER BY started_at`,
    [paymentId]
  )

  return rows
}
