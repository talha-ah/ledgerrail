import type { PoolClient } from "pg"

import type { Hold } from "../../types.js"
import { toPostings, type Leg } from "../guards.js"
import { PermanentError } from "../../utils/error.js"
import {
  appendEvent,
  type AppendResult,
  findByIdempotencyKey,
  applyToAccountBalances,
} from "../appendEvent.js"

/**
 * Derived, like `stepKey`, and one level down. A release is requested by a
 * compensating payment *and* by the sweeper, and the two must present the same
 * key or the journal takes two `HoldReleased` events for one hold. Keyed by the
 * hold rather than the payment because the fact recorded is about the hold —
 * the sweeper does not always have a payment to name.
 */
export const holdKey = (holdId: string, action: "release") =>
  `hold:${holdId}:${action}`

/**
 * Claims the hold for this settlement. FOR UPDATE plus `status = 'held'` is the
 * whole over-release guard: a second consumer finds no row and is refused, and a
 * concurrent one waits and then finds the status changed.
 *
 * `requireLive` is true for a capture and false for a release, and the asymmetry
 * is the point: expiry bounds how long money may be *held*, not how long it may
 * be *given back*. Refusing to return a customer's funds because the reservation
 * timed out strands the money the reservation existed to protect — and 3.4 walks
 * straight into it, because a capture that fails on expiry abandons to
 * `compensating`, whose only move is a release that would fail for the same
 * reason.
 *
 * Both refusals are permanent now. `status` never returns to 'held' and time only
 * moves forward, so no retry can help either — left transient they burned the
 * whole budget before compensating.
 */
async function claimHold(
  holdId: string,
  client: PoolClient,
  requireLive: boolean
): Promise<Hold> {
  const { rows } = await client.query<Hold>(
    "SELECT * FROM holds WHERE hold_id = $1 AND status = 'held' FOR UPDATE",
    [holdId]
  )

  const hold = rows[0]
  if (!hold) throw new PermanentError("Hold is not available to settle")

  if (requireLive && hold.expires_at.getTime() < Date.now())
    throw new PermanentError("Hold has expired")

  return hold
}

async function settle(
  holdId: string,
  destination: number | "source",
  idempotencyKey: string,
  eventType: "HoldCaptured" | "HoldReleased",
  status: "captured" | "released",
  client: PoolClient
): Promise<AppendResult> {
  // Before the guard, not after — the ordering holdFunds has had since 1.4 and
  // this never did. Nothing called either of these twice before now; 3.4 calls
  // release from two places under one derived key, so the second caller must be
  // handed the original receipt rather than told the hold is no longer 'held'.
  // Exactly the shape of the bug 3.1 found in postTransaction, one file over.
  //
  // A *different* key settling the same hold still throws, which is what
  // holdFunds.test.ts:279 asserts and what the hold's status transition is for.
  const replay = await findByIdempotencyKey(idempotencyKey, client)
  if (replay) return replay

  const hold = await claimHold(holdId, client, eventType === "HoldCaptured")

  const toAccountId = destination === "source" ? hold.account_id : destination

  // Out of the reserve, into wherever this settlement sends it. Never an undo:
  // the hold posting stands in the journal forever and this is a new fact.
  const legs: Leg[] = [
    {
      accountId: hold.hold_account_id,
      amount: -hold.amount,
      currency: hold.currency,
    },
    { accountId: toAccountId, amount: hold.amount, currency: hold.currency },
  ]

  const payload = { holdId, idempotencyKey, postings: toPostings(legs) }

  const appended = await appendEvent(
    eventType,
    1,
    payload,
    idempotencyKey,
    client
  )
  if (appended.replayed) return appended

  await applyToAccountBalances(payload.postings, client)

  const { rowCount } = await client.query(
    `UPDATE holds
        SET status = $2, settled_event_id = $3, updated_at = NOW()
      WHERE hold_id = $1 AND status = 'held'`,
    [holdId, status, appended.eventId]
  )
  if (rowCount !== 1) throw new Error("Hold was settled concurrently")

  return appended
}

export function captureHold(
  holdId: string,
  destinationAccountId: number,
  idempotencyKey: string,
  client: PoolClient
): Promise<AppendResult> {
  return settle(
    holdId,
    destinationAccountId,
    idempotencyKey,
    "HoldCaptured",
    "captured",
    client
  )
}

/**
 * The destination is resolved inside `settle`, from the hold it already claimed.
 * Reading it out here first — as this did — meant claiming the hold before the
 * replay check, which is the same bug wearing a different hat.
 */
export function releaseHold(
  holdId: string,
  idempotencyKey: string,
  client: PoolClient
): Promise<AppendResult> {
  return settle(
    holdId,
    "source",
    idempotencyKey,
    "HoldReleased",
    "released",
    client
  )
}
