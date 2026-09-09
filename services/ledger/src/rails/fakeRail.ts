import { createHash, randomUUID } from "node:crypto"

import { PermanentError } from "../utils/error.js"
import { convert, money, parseRate, type Currency } from "@ledgerrail/money"
import type {
  Rail,
  QuoteRequest,
  QuoteResponse,
  SettleRequest,
  SettleResponse,
  ReverseRequest,
  ReverseResponse,
} from "./types.js"

const IDENTITY_RATE = "1.0000000000"

const DEFAULT_RATES: Record<string, string> = {
  "PKR>USD": "0.0035000000",
  "USD>PKR": "285.0000000000",
  "GBP>USD": "1.2600000000",
  "USD>GBP": "0.7900000000",
  "EUR>USD": "1.0800000000",
  "USD>EUR": "0.9200000000",
}

const pair = (from: Currency, to: Currency) => `${from}>${to}`

const isLive = (expiresAt: string) => Date.parse(expiresAt) > Date.now()

/**
 * What the key was first used to ask, so a replay can be checked rather than
 * assumed — the hole `request_fingerprint` closed in `startPayment`, on the side
 * of the boundary we do not own. A fake that answers a *different* request with
 * the original response lets an idempotency test pass while proving nothing.
 */
function fingerprint(parts: (string | number)[]): string {
  return createHash("sha256").update(parts.join("\u0000")).digest("hex")
}

interface Replayed<T> {
  fingerprint: string
  response: T
}

interface Quoted {
  request: QuoteRequest
  response: QuoteResponse
}

export interface FakeRailOptions {
  rates?: Record<string, string>
  quoteTtlMs?: number
  /**
   * Runs before the rail does anything, so a throw here is a call that never
   * landed — the *unambiguous* failure, and the only one 3.3 needs. Staging the
   * ambiguous one (settled, then the answer is lost) needs a hook on the far
   * side of the write below, and that is 3.5's.
   */
  beforeSettle?: (req: SettleRequest) => Promise<void> | void
  /** The mirror of beforeSettle, so a compensation can be made to fail too. */
  beforeReverse?: (req: ReverseRequest) => Promise<void> | void
}

/**
 * A fake's failure mode is being more permissive than the thing it stands in
 * for, so every refusal below is deliberate: an unknown quote, an expired one, a
 * settlement that does not match its quote, a key reused for another request.
 * Each check it skipped would be a test that proved nothing.
 *
 * `PermanentError` rather than `Error` where a retry cannot help. That is the
 * contract 3.0's HTTP adapter inherits — it maps status codes onto the same
 * distinction, and the orchestrator only ever sees this one.
 */
export function createFakeRail(options: FakeRailOptions = {}): Rail {
  const rates = { ...DEFAULT_RATES, ...options.rates }
  const quoteTtlMs = options.quoteTtlMs ?? 60_000

  // At construction, not mid-saga. A malformed rate is a configuration fault,
  // and `parseRate` throws a plain Error — which `isPermanent` reads as
  // transient, so discovering it inside a payment burns the whole retry budget
  // before compensating something that was never going to work.
  for (const rate of Object.values(rates)) parseRate(rate)

  const quotes = new Map<string, Replayed<QuoteResponse>>()
  const byQuoteId = new Map<string, Quoted>()
  const settlements = new Map<string, Replayed<SettleResponse>>()
  const reversals = new Map<string, Replayed<ReverseResponse>>()

  return {
    async quote(req: QuoteRequest): Promise<QuoteResponse> {
      const fp = fingerprint([req.from, req.to, req.amount])
      const replay = quotes.get(req.idempotencyKey)

      if (replay) {
        if (replay.fingerprint !== fp) {
          throw new PermanentError(
            `Quote key ${req.idempotencyKey} reused for another request`
          )
        }

        // Replayed only while it is still live. A payment resumed after a crash
        // must get the rate its customer was already told — but an expired quote
        // has to be re-priced, because that is what expiry means. Returning the
        // dead one would make it unrefreshable for the life of the payment: the
        // key is `${paymentId}:quote` on every attempt, not per attempt.
        if (isLive(replay.response.expiresAt)) return replay.response
      }

      const rate =
        req.from === req.to ? IDENTITY_RATE : rates[pair(req.from, req.to)]

      if (!rate) {
        throw new PermanentError(`No rate for ${pair(req.from, req.to)}`)
      }

      const targetAmount = convert(
        money(BigInt(req.amount), req.from),
        rate,
        req.to
      ).amount

      // The source amount is floored by a CHECK; the target is floored by
      // nothing, and truncation can eat a small payment entirely. Settling it
      // would move the source money and deliver none.
      if (targetAmount <= 0n) {
        throw new PermanentError(
          `${req.amount} ${req.from} rounds to nothing in ${req.to}`
        )
      }

      const response: QuoteResponse = {
        quoteId: `qt_${randomUUID()}`,
        rate,
        targetAmount: String(targetAmount),
        expiresAt: new Date(Date.now() + quoteTtlMs).toISOString(),
      }

      quotes.set(req.idempotencyKey, { fingerprint: fp, response })
      byQuoteId.set(response.quoteId, { request: req, response })

      return response
    },

    async settle(req: SettleRequest): Promise<SettleResponse> {
      // Deliberately without quoteId: an expired quote is re-priced into a new
      // one, so hashing it would make the *replay* of a settlement that already
      // happened look like a different request — refusing the one call that had
      // to succeed. A fingerprint identifies the payment, never the attempt,
      // which is the same rule that keeps stepKey per step.
      const fp = fingerprint([
        req.destinationReference,
        req.amount,
        req.currency,
        req.targetCurrency,
      ])

      const replay = settlements.get(req.idempotencyKey)

      if (replay) {
        if (replay.fingerprint !== fp) {
          throw new PermanentError(
            `Settle key ${req.idempotencyKey} reused for another request`
          )
        }

        // A reversed settlement is not a settled one. Unreachable from the
        // orchestrator — nothing returns to `settling` once compensation starts
        // — and refused anyway, because a fake that quietly answers "settled"
        // to a key it has already sent back is more permissive than any rail.
        if (replay.response.status === "reversed") {
          throw new PermanentError(
            `Settle key ${req.idempotencyKey} was reversed`
          )
        }

        // Before the injected failure and before the quote check below: a key
        // that has already settled is answered from the map no matter what,
        // because a rail that can be made to fail a *replay* deduplicates only
        // when nothing goes wrong — the one case it is not for. This is also
        // what makes a stale quote survivable after the money has moved.
        return replay.response
      }

      const quoted = byQuoteId.get(req.quoteId)
      if (!quoted) throw new PermanentError(`Unknown quote ${req.quoteId}`)

      // The check that makes the orchestrator's backoff honest. Without it the
      // ceiling (5 min) quietly outlives the quote (60 s) and every retry past
      // the second settles against a dead price a real rail would refuse.
      if (!isLive(quoted.response.expiresAt)) {
        throw new PermanentError(`Quote ${req.quoteId} has expired`)
      }

      // A quote prices one specific conversion. Letting a settlement cite a
      // quote it does not match is how an orchestrator holding both a source and
      // a target amount sends the wrong one and is told it was fine.
      if (
        quoted.request.from !== req.currency ||
        quoted.request.to !== req.targetCurrency ||
        quoted.request.amount !== req.amount
      ) {
        throw new PermanentError(
          `Settlement does not match quote ${req.quoteId}`
        )
      }

      await options.beforeSettle?.(req)

      const response: SettleResponse = {
        railReference: `rail_${randomUUID()}`,
        status: "settled",
      }

      settlements.set(req.idempotencyKey, { fingerprint: fp, response })

      return response
    },

    async lookup(idempotencyKey: string): Promise<SettleResponse | null> {
      return settlements.get(idempotencyKey)?.response ?? null
    },

    async reverse(req: ReverseRequest): Promise<ReverseResponse> {
      const fp = fingerprint([
        req.settleIdempotencyKey,
        req.amount,
        req.currency,
      ])

      const replay = reversals.get(req.idempotencyKey)

      if (replay) {
        if (replay.fingerprint !== fp) {
          throw new PermanentError(
            `Reverse key ${req.idempotencyKey} reused for another request`
          )
        }

        return replay.response
      }

      await options.beforeReverse?.(req)

      const settled = settlements.get(req.settleIdempotencyKey)

      const response: ReverseResponse = settled
        ? { railReference: `rev_${randomUUID()}`, status: "reversed" }
        : { railReference: "", status: "not_found" }

      // Marked, not deleted. `lookup` is 3.5's whole interface to what the rail
      // believes, and a reversal that erases the settlement leaves the
      // reconciler unable to distinguish "sent back" from "never happened" —
      // the one distinction it is being written to make.
      if (settled) {
        settlements.set(req.settleIdempotencyKey, {
          fingerprint: settled.fingerprint,
          response: { ...settled.response, status: "reversed" },
        })
      }

      reversals.set(req.idempotencyKey, { fingerprint: fp, response })

      return response
    },
  }
}
