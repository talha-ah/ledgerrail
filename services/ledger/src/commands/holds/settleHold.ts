import type { PoolClient } from "pg"

import type { Hold } from "../../types.js"
import { toPostings, type Leg } from "../guards.js"
import { appendEvent, applyToAccountBalances } from "../appendEvent.js"

/**
 * Claims the hold for this settlement. FOR UPDATE plus `status = 'held'` is the
 * whole over-release guard: a second capture finds no row and is refused, and a
 * concurrent one waits and then finds the status changed. This is where the
 * invariant lives, rather than on the reserve account's balance — locking that
 * would serialise every hold in the system on one row.
 */
async function claimHold(holdId: string, client: PoolClient): Promise<Hold> {
  const { rows } = await client.query<Hold>(
    "SELECT * FROM holds WHERE hold_id = $1 AND status = 'held' FOR UPDATE",
    [holdId]
  )

  const hold = rows[0]
  if (!hold) throw new Error("Hold is not available to settle")

  if (hold.expires_at.getTime() < Date.now())
    throw new Error("Hold has expired")

  return hold
}

async function settle(
  holdId: string,
  toAccountId: number,
  idempotencyKey: string,
  eventType: "HoldCaptured" | "HoldReleased",
  status: "captured" | "released",
  client: PoolClient
) {
  const hold = await claimHold(holdId, client)

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

  const payload = {
    holdId,
    idempotencyKey,
    postings: toPostings(legs),
  }

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
) {
  return settle(
    holdId,
    destinationAccountId,
    idempotencyKey,
    "HoldCaptured",
    "captured",
    client
  )
}

export function releaseHold(
  holdId: string,
  idempotencyKey: string,
  client: PoolClient
) {
  return claimHold(holdId, client).then((hold) =>
    settle(
      holdId,
      hold.account_id,
      idempotencyKey,
      "HoldReleased",
      "released",
      client
    )
  )
}
