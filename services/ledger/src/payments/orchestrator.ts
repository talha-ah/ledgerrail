import type { PoolClient } from "pg"

import { stepKey } from "./states.js"
import type { Currency } from "@ledgerrail/money"
import { holdFunds } from "../commands/holds/holdFunds.js"
import type { Payment, PaymentState, PaymentStepName } from "../types.js"
import {
  holdKey,
  captureHold,
  releaseHold,
} from "../commands/holds/settleHold.js"
import type {
  Rail,
  QuoteRequest,
  SettleRequest,
  ReverseRequest,
} from "../rails/index.js"
import {
  errorCode,
  isPermanent,
  errorMessage,
  PermanentError,
} from "../utils/error.js"
import {
  advance,
  backoffMs,
  beginStep,
  finishStep,
  takeLease,
  listSteps,
  countSteps,
  getPayment,
  leaseUntil,
  reschedule,
  MAX_ATTEMPTS,
  claimPayments,
} from "./payments.js"

type Transactor = <T>(cb: (client: PoolClient) => Promise<T>) => Promise<T>

export interface OrchestratorDeps {
  rail: Rail
  transaction: Transactor
  /** How much life a quote must have left to be worth settling against. */
  quoteMarginMs?: number
}

/**
 * A hold must outlive the saga that took it, or 3.4's sweeper releases money a
 * payment is still trying to send. Comfortably longer than MAX_ATTEMPTS of
 * backoff, short enough that a saga abandoned without compensation still returns
 * the customer's money the same day.
 */
export const HOLD_TTL_MS = 24 * 60 * 60 * 1000

/**
 * How long a payment this tick cannot drive is left alone. `compensating` is not
 * terminal, so `claimPayments` keeps handing it back until 3.4 exists — and an
 * orchestrator that claims a state it has no handler for is a hot loop against
 * its own database. The default branch is load-bearing.
 */
const UNHANDLED_BACKOFF_MS = 60 * 60 * 1000

/**
 * A quote that expires *during* the call is refused by the rail, and the refusal
 * arrives after `settling` is committed — from where the only way out is
 * forward, through a compensation. Re-pricing early is one extra FX call;
 * discovering it late costs a customer their payment.
 */
const QUOTE_MARGIN_MS = 5_000

/** How many times a payment may be re-priced before it is not going to settle. */
const MAX_REPRICES = 5

export type TickResult =
  | { payment: string; outcome: "advanced"; state: PaymentState }
  | { payment: string; outcome: "retry"; error: string }
  | {
      payment: string
      outcome: "abandoned"
      state: PaymentState
      error: string
    }
  | { payment: string; outcome: "parked"; state: PaymentState }
  | { payment: string; outcome: "lost" }

async function railAccount(
  currency: Currency,
  client: PoolClient
): Promise<number> {
  const { rows } = await client.query<{ id: number }>(
    "SELECT id FROM accounts WHERE account_number = $1",
    [`RAIL:${currency}`]
  )

  if (!rows[0]) throw new Error(`No rail clearing account for ${currency}`)

  return rows[0].id
}

function quoteExpired(payment: Payment, margin: number): boolean {
  if (!payment.quote_expires_at) return true
  return payment.quote_expires_at.getTime() - margin <= Date.now()
}

/**
 * `initiated → held` in ONE transaction, and that is the point rather than an
 * optimisation. The saga machinery exists because you cannot atomically call a
 * bank and record that you called it; a hold is a write to our own database, so
 * atomicity is still available — and begin/act/finish here would be ceremony
 * that buys nothing and opens a crash window that did not have to exist.
 */
async function takeHold(payment: Payment, deps: OrchestratorDeps) {
  await deps.transaction(async (client) => {
    // stepKey with no step row: the hold takes no `payment_steps` entry because
    // nothing about it is ambiguous, but the key still has to be derived rather
    // than generated or a retry would reserve the funds twice.
    const { holdId } = await holdFunds(
      {
        idempotencyKey: stepKey(payment.payment_id, "hold"),
        accountId: payment.source_account_id,
        amount: payment.amount,
        currency: payment.currency,
        expiresAt: new Date(Date.now() + HOLD_TTL_MS),
      },
      client
    )

    await advance(payment.payment_id, "initiated", "held", client, {
      holdId,
      resetAttempts: true,
      nextAttemptAt: new Date(),
    })
  })
}

/**
 * Intent, call, outcome — the three-phase shape, used only where it is the only
 * shape that survives a crash between a commit and a call we do not control.
 *
 * The failure path records the outcome too. A step left `started` means "we
 * never learned what happened"; a step we *did* learn failed is a different
 * fact, and collapsing them would have 3.5's reconciler interrogating the rail
 * about calls it already knows were refused.
 */
async function callRail<T>(
  payment: Payment,
  step: PaymentStepName,
  request: unknown,
  call: () => Promise<T>,
  deps: OrchestratorDeps,
  alsoInIntent?: (client: PoolClient) => Promise<unknown>
): Promise<{ attempt: number; response: T }> {
  const { attempt } = await deps.transaction(async (client) => {
    await alsoInIntent?.(client)
    return beginStep(payment.payment_id, step, request, client)
  })

  try {
    return { attempt, response: await call() }
  } catch (err) {
    // Only a *learned* refusal is recorded. A transport error means the call may
    // have reached the bank and the answer was lost, which is what a `started`
    // row means — and what `unfinishedSteps` hands 3.5. Stamping it `failed`
    // says the rail answered when it did not, so the reconciler finds nothing
    // to ask about and the hold is released while the money is gone.
    if (isPermanent(err)) {
      try {
        await deps.transaction((client) =>
          finishStep(
            payment.payment_id,
            step,
            attempt,
            { status: "failed", error: errorMessage(err) },
            client
          )
        )
      } catch {
        // The rail's error is the one that matters and a failure to record it
        // must not replace it. The step stays `started`, which is the safe
        // reading — "we do not know" is never worse than the truth.
      }
    }

    throw err
  }
}

/**
 * A quote moves no money, so its ambiguity is cheap — which is exactly why there
 * is no `quoting` state. A state must name a crash window that recovers
 * differently from its neighbours, and the recovery here is "ask again". The
 * step row is written regardless, because the audit trail should not have a hole
 * where the FX call was.
 */
async function getQuote(payment: Payment, deps: OrchestratorDeps) {
  const request: QuoteRequest = {
    idempotencyKey: stepKey(payment.payment_id, "quote"),
    from: payment.currency,
    to: payment.target_currency,
    amount: String(payment.amount),
  }

  const { attempt, response } = await callRail(
    payment,
    "quote",
    request,
    () => deps.rail.quote(request),
    deps
  )

  await deps.transaction(async (client) => {
    await finishStep(
      payment.payment_id,
      "quote",
      attempt,
      { status: "succeeded", response },
      client
    )

    // A quote born inside the margin is not handled here. It advances like any
    // other and the `quoted` branch catches it — one path for every stale quote
    // rather than two that have to agree about what stale means.
    await advance(payment.payment_id, "held", "quoted", client, {
      quoteId: response.quoteId,
      quoteRate: response.rate,
      quoteExpiresAt: new Date(response.expiresAt),
      resetAttempts: true,
      nextAttemptAt: new Date(),
    })
  })
}

async function settlePayment(payment: Payment, deps: OrchestratorDeps) {
  if (!payment.quote_id) {
    throw new PermanentError(`Payment ${payment.payment_id} has no quote`)
  }

  // A pure function of the payment row, deliberately. A retry must send an
  // identical request, and a reconciler that finds this payment mid-settlement
  // must be able to reconstruct what was sent without reading a JSONB blob —
  // the same reasoning that makes stepKey derived rather than generated.
  const request: SettleRequest = {
    idempotencyKey: stepKey(payment.payment_id, "settle"),
    quoteId: payment.quote_id,
    destinationReference: payment.destination_reference,
    amount: String(payment.amount),
    currency: payment.currency,
    targetCurrency: payment.target_currency,
  }

  const { attempt, response } = await callRail(
    payment,
    "settle",
    request,
    () => deps.rail.settle(request),
    deps,
    // In the SAME transaction as the intent. Not required for safety — the key
    // is derivable from the payment row, which is why it is derived — but it
    // keeps "entered the ambiguous window" and "recorded what we were about to
    // send" from ever disagreeing, and spares a reconciler the reconstruction.
    //
    // A payment claimed already in `settling` is a resumed one: the window was
    // entered by a worker that died, so there is no transition left to make.
    payment.state === "quoted"
      ? (client) =>
          advance(payment.payment_id, "quoted", "settling", client, {
            // Neither bumped nor reset: entering the ambiguous window is the
            // middle of the settle step, not the end of one.
            nextAttemptAt: leaseUntil(),
          })
      : undefined
  )

  await deps.transaction(async (client) => {
    await finishStep(
      payment.payment_id,
      "settle",
      attempt,
      { status: "succeeded", response },
      client
    )

    await advance(payment.payment_id, "settling", "settled", client, {
      resetAttempts: true,
      nextAttemptAt: new Date(),
    })
  })
}

/**
 * `settled → completed`, one transaction again and for the hold's reason: both
 * halves are ours. The money leaves the reserve for the clearing account that
 * stands for "handed to the rail" — never for the destination itself, which is
 * an IBAN at an institution whose balance this ledger does not keep.
 */
async function capture(payment: Payment, deps: OrchestratorDeps) {
  if (!payment.hold_id) {
    throw new PermanentError(`Payment ${payment.payment_id} has no hold`)
  }

  await deps.transaction(async (client) => {
    const destination = await railAccount(payment.currency, client)

    await captureHold(
      payment.hold_id!,
      destination,
      stepKey(payment.payment_id, "capture"),
      client
    )

    await advance(payment.payment_id, "settled", "completed", client, {
      resetAttempts: true,
    })
  })
}

async function requote(payment: Payment, deps: OrchestratorDeps) {
  const priced = await deps.transaction((client) =>
    countSteps(payment.payment_id, "quote", client)
  )

  // `attempts` cannot bound this loop and should not: it is reset by the quote
  // that ends each lap, which is right — a fresh quote is a fresh step budget.
  // The append-only step log is the counter that never resets, which is what an
  // append-only log is for. Unbounded, a rail whose TTL sits under our margin
  // re-prices forever with `attempts` reading zero.
  if (priced >= MAX_REPRICES) {
    throw new PermanentError(
      `Re-priced ${priced} times without getting a quote long-lived enough to settle`
    )
  }

  // The rail replays a quote while it is still technically live, and the margin
  // calls it dead earlier — so re-asking inside that window returns the same
  // dead-on-arrival price. Wait for it to genuinely expire, then ask: one sleep
  // instead of a loop.
  //
  // Neither bumped nor reset: a re-pricing is not an attempt at a side effect,
  // and the count above is what bounds it.
  const expiry = payment.quote_expires_at?.getTime() ?? 0
  const notBefore = expiry > Date.now() ? new Date(expiry + 1_000) : new Date()

  await deps.transaction((client) =>
    advance(payment.payment_id, "quoted", "held", client, {
      nextAttemptAt: notBefore,
      lastError: "Quote too close to expiry to settle",
    })
  )
}

async function park(payment: Payment, deps: OrchestratorDeps) {
  await deps.transaction((client) =>
    reschedule(payment.payment_id, payment.state, payment.lease_token, client, {
      nextAttemptAt: new Date(Date.now() + UNHANDLED_BACKOFF_MS),
      lastError: payment.last_error ?? `No handler for ${payment.state}`,
    })
  )
}

/**
 * From `initiated` nothing has moved, so there is nothing to give back and
 * `failed` is honest. From anywhere else a hold exists — and past `settled`,
 * money has actually left the building — so the only correct exit is through
 * compensation. That is why the transition table has no edge from those states
 * straight to `failed`: skipping compensation strands a customer's money.
 */
function abandonTo(state: PaymentState): PaymentState {
  if (state === "initiated") return "failed"

  // A compensation that cannot finish has nowhere to compensate *to*. `failed`
  // is the honest terminal, and it is only safe because the sweeper is behind
  // it: the hold outlives the payment, expires, and is released once no live
  // payment claims it. Returning null rescheduled forever, which looked like
  // patience and was a hot loop with a clean record.
  if (state === "compensating") return "failed"

  return "compensating"
}

async function recordFailure(
  payment: Payment,
  err: unknown,
  deps: OrchestratorDeps
): Promise<TickResult> {
  const message = errorMessage(err)

  // Re-read, because the state the handler *started* in is not the state the
  // failure happened in: the settle handler commits `settling` before the call,
  // so a rail error arrives with the payment one state further on — and its
  // lease pushed forward too. Fencing on the stale values would miss every time.
  const current = await deps.transaction((client) =>
    getPayment(payment.payment_id, client)
  )

  if (!current) return { payment: payment.payment_id, outcome: "lost" }

  try {
    const to = abandonTo(current.state)
    if (isPermanent(err) || current.attempts + 1 >= MAX_ATTEMPTS) {
      await deps.transaction((client) =>
        advance(current.payment_id, current.state, to, client, {
          lastError: message,
          nextAttemptAt: new Date(),
          // A payment abandoned after five settle failures enters `compensating`
          // with the budget already spent, so the first transient reverse failure
          // ends it immediately. Abandonment starts a different kind of work and
          // gets its own budget — the same conflation 3.3 found on the successful
          // moves, one path over.
          resetAttempts: true,
          // From `payment`, not `current`: the token is what this worker was
          // handed at claim time, and re-reading it is what would make the fence
          // tautological. Needed here and nowhere else — a forward transition
          // fences itself by changing the state, but abandonment leaves from a
          // state the rightful owner is still working in.
          leaseToken: payment.lease_token,
        })
      )

      return {
        payment: current.payment_id,
        outcome: "abandoned",
        state: to,
        error: message,
      }
    }

    await deps.transaction((client) =>
      reschedule(
        current.payment_id,
        current.state,
        payment.lease_token,
        client,
        {
          nextAttemptAt: new Date(Date.now() + backoffMs(current.attempts + 1)),
          lastError: message,
          bumpAttempts: true,
        }
      )
    )

    return { payment: current.payment_id, outcome: "retry", error: message }
  } catch (err) {
    // Losing the fence and failing to write are different failures. A SQLSTATE
    // means the database refused us — a lock timeout, a dropped connection —
    // not that another worker took the payment. Reported as `lost` it writes no
    // error, no attempt and no backoff, so the payment quietly retries every
    // lease period forever with a clean record and a budget it never spends.
    if (errorCode(err) !== undefined) throw err

    // The fence refused us: another worker took this payment while we were
    // failing at it. Its verdict stands — ours is the stale one.
    return { payment: payment.payment_id, outcome: "lost" }
  }
}

/**
 * Whether the rail has to be asked to send the money back.
 *
 * `payments` holds current state, so by the time it reads `compensating` the
 * state it was abandoned from is gone — and that is the wrong thing to ask
 * anyway. The step log answers it directly and append-only: a settle attempt
 * exists iff we tried it, and a `failed` one is the rail's own "no". Anything
 * else — `succeeded`, or `started` and never resolved — means the money may be
 * gone, and "may be" is treated exactly like "is". The same rule callRail obeys
 * when it declines to stamp a transport error as a refusal.
 */
async function settlementMayHaveLanded(
  payment: Payment,
  deps: OrchestratorDeps
): Promise<boolean> {
  const steps = await deps.transaction((client) =>
    listSteps(payment.payment_id, "settle", client)
  )

  return steps.some((step) => step.status !== "failed")
}

/**
 * Asks the rail to send it back. Intent, call, outcome — the same three phases
 * as the settle, because it is the same kind of call: one we do not control,
 * whose answer can be lost.
 *
 * Re-called on resume rather than skipped when a reverse step already succeeded,
 * for the reason the settle is: one code path, and the key is what makes a
 * second call harmless. A `not_found` is a *succeeded* step — we asked, and
 * learned there was nothing there.
 */
async function reverseSettlement(payment: Payment, deps: OrchestratorDeps) {
  const request: ReverseRequest = {
    idempotencyKey: stepKey(payment.payment_id, "reverse"),
    settleIdempotencyKey: stepKey(payment.payment_id, "settle"),
    amount: String(payment.amount),
    currency: payment.currency,
  }

  const { attempt, response } = await callRail(
    payment,
    "reverse",
    request,
    () => deps.rail.reverse(request),
    deps
  )

  await deps.transaction((client) =>
    finishStep(
      payment.payment_id,
      "reverse",
      attempt,
      { status: "succeeded", response },
      client
    )
  )
}

/**
 * `compensating → compensated`.
 *
 * A compensation is a forward transaction. Nothing is deleted and no event is
 * amended: the bank is asked to send the money back, and the reserve is emptied
 * by a new `HoldReleased` posting that sits in the journal beside the settle it
 * answers. You cannot un-send money; you send it back — which is why 3.4 adds no
 * event type. The ledger movement of a compensation is one the ledger already
 * had.
 */
async function compensate(payment: Payment, deps: OrchestratorDeps) {
  if (!payment.hold_id) {
    throw new PermanentError(
      `Payment ${payment.payment_id} is compensating with nothing held`
    )
  }

  if (await settlementMayHaveLanded(payment, deps)) {
    await reverseSettlement(payment, deps)
  }

  await deps.transaction(async (client) => {
    await releaseHold(
      payment.hold_id!,
      holdKey(payment.hold_id!, "release"),
      client
    )

    await advance(payment.payment_id, "compensating", "compensated", client, {
      resetAttempts: true,
      // Carried, not cleared. `advance` clears last_error on a successful move,
      // which is right everywhere else and wrong here: why this payment did not
      // go through is the only thing anyone will ask a compensated row.
      lastError: payment.last_error,
    })
  })
}

/**
 * Drives one payment by one *step*. Not to completion, and not by one state:
 * `settling` is the middle of the settle step, not a resting place.
 *
 * One step per tick is what makes the resumer and the driver the same code. A
 * payment claimed in `settling` is not a special recovery path — it is this
 * switch, on that state, doing what it always does.
 */
async function drive(
  payment: Payment,
  deps: OrchestratorDeps
): Promise<TickResult> {
  const id = payment.payment_id

  try {
    switch (payment.state) {
      case "initiated":
        await takeHold(payment, deps)
        return { payment: id, outcome: "advanced", state: "held" }

      case "held":
        await getQuote(payment, deps)
        return { payment: id, outcome: "advanced", state: "quoted" }

      case "quoted":
        if (quoteExpired(payment, deps.quoteMarginMs ?? QUOTE_MARGIN_MS)) {
          await requote(payment, deps)
          return { payment: id, outcome: "advanced", state: "held" }
        }

        await settlePayment(payment, deps)
        return { payment: id, outcome: "advanced", state: "settled" }

      case "settling":
        await settlePayment(payment, deps)
        return { payment: id, outcome: "advanced", state: "settled" }

      case "settled":
        await capture(payment, deps)
        return { payment: id, outcome: "advanced", state: "completed" }

      case "compensating":
        await compensate(payment, deps)
        return { payment: id, outcome: "advanced", state: "compensated" }

      default:
        await park(payment, deps)
        return { payment: id, outcome: "parked", state: payment.state }
    }
  } catch (err) {
    return recordFailure(payment, err, deps)
  }
}

export async function tick(
  deps: OrchestratorDeps,
  limit = 10
): Promise<TickResult[]> {
  const claimed = await deps.transaction(async (client) => {
    const candidates = await claimPayments(client, limit)
    const taken: Payment[] = []

    for (const candidate of candidates) {
      // Under FOR UPDATE this fence cannot fail. It is here so the lease is
      // written by the same function that later checks it, rather than by two
      // statements that have to be kept agreeing by hand.
      const leased = await takeLease(candidate, client)
      if (leased) taken.push(leased)
    }

    return taken
  })

  const results: TickResult[] = []

  // Sequential, not Promise.all: each drive holds a pool connection for the
  // length of a transaction, and a full batch of ten against a pool of ten would
  // deadlock the worker against itself the moment a handler needs a second one.
  for (const payment of claimed) {
    results.push(await drive(payment, deps))
  }

  return results
}

export async function runWorker(
  deps: OrchestratorDeps,
  options: { intervalMs?: number; signal?: AbortSignal } = {}
): Promise<void> {
  const intervalMs = options.intervalMs ?? 1_000

  while (!options.signal?.aborted) {
    try {
      const results = await tick(deps)

      // Only sleep when there was nothing to do. A backlog drains as fast as the
      // rail allows; the interval is an idle poll, not a rate limit.
      if (results.length === 0) await sleep(intervalMs)
    } catch (err) {
      console.error("[Orchestrator] tick failed:", errorMessage(err))
      await sleep(intervalMs)
    }
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
