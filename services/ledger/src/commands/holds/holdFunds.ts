import type { PoolClient } from "pg"
import { randomUUID } from "node:crypto"

import type { Currency } from "@ledgerrail/money"
import type { Hold, FundsHeldPayloadV1 } from "../../types.js"
import { assertPostable, toPostings, type Leg } from "../guards.js"
import {
  appendEvent,
  applyToAccountBalances,
  findByIdempotencyKey,
} from "../appendEvent.js"

export interface HoldFundsInput {
  idempotencyKey: string
  accountId: number
  amount: bigint
  currency: Currency
  expiresAt: Date
}

async function heldFundsAccount(currency: Currency, client: PoolClient) {
  const { rows } = await client.query<{ id: number }>(
    "SELECT id FROM accounts WHERE account_number = $1",
    [`HELD:${currency}`]
  )
  if (!rows[0]) throw new Error(`No held-funds account for ${currency}`)
  return rows[0].id
}

export async function holdFunds(
  input: HoldFundsInput,
  client: PoolClient
): Promise<{ holdId: string; eventId: string; globalSeq: string }> {
  if (input.amount <= 0n) throw new Error("Invalid hold amount")

  const holdAccountId = await heldFundsAccount(input.currency, client)

  // A hold is a posting like any other: the money leaves the customer's balance
  // for a reserve it has not yet left the building from. That is what makes it
  // replayable, auditable and covered by Σ = 0 — none of which is true of a
  // number in a mutable column.
  const legs: Leg[] = [
    {
      accountId: input.accountId,
      amount: -input.amount,
      currency: input.currency,
    },
    {
      accountId: holdAccountId,
      amount: input.amount,
      currency: input.currency,
    },
  ]

  // Before the guard, not after: a replay must return what the original did.
  // Re-checking funds the original already consumed turns a successful retry
  // into "Insufficient funds", which is exactly the failure 1.4 removed.
  const replay = await findByIdempotencyKey(input.idempotencyKey, client)
  if (replay) {
    const { rows } = await client.query<Pick<Hold, "hold_id">>(
      "SELECT hold_id FROM holds WHERE held_event_id = $1",
      [replay.eventId]
    )
    if (!rows[0]) throw new Error("Replayed hold has no hold row")
    return { ...replay, holdId: rows[0].hold_id }
  }

  await assertPostable(legs, client)

  const holdId = randomUUID()

  const payload: FundsHeldPayloadV1 = {
    holdId,
    idempotencyKey: input.idempotencyKey,
    expiresAt: input.expiresAt.toISOString(),
    postings: toPostings(legs),
  }

  const appended = await appendEvent(
    "FundsHeld",
    1,
    payload,
    input.idempotencyKey,
    client
  )

  if (appended.replayed) {
    // Same key, same hold. Return the original rather than reserving twice.
    const { rows } = await client.query<Pick<Hold, "hold_id">>(
      "SELECT hold_id FROM holds WHERE held_event_id = $1",
      [appended.eventId]
    )
    if (!rows[0]) throw new Error("Replayed hold has no hold row")

    return { ...appended, holdId: rows[0].hold_id }
  }

  await applyToAccountBalances(payload.postings, client)

  await client.query(
    `
    INSERT INTO holds (hold_id, account_id, hold_account_id, amount, currency, status, held_event_id, expires_at)
    VALUES ($1, $2, $3, $4, $5, 'held', $6, $7)`,
    [
      holdId,
      input.accountId,
      holdAccountId,
      input.amount,
      input.currency,
      appended.eventId,
      input.expiresAt,
    ]
  )

  return { ...appended, holdId }
}
