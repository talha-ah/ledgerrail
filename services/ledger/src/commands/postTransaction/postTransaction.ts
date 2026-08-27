import type { PoolClient } from "pg"

import type { Currency } from "@ledgerrail/money"

import { LATEST_EVENT_VERSION } from "../../events/index.js"
import type { TransactionPostedPayloadV2 } from "../../types.js"
import { assertPostable, toPostings, type Leg } from "../guards.js"
import {
  appendEvent,
  findByIdempotencyKey,
  applyToAccountBalances,
} from "../appendEvent.js"

interface Posting {
  accountId: number
  amount: bigint // negative or positive
}

export interface PostTransactionInput {
  idempotencyKey: string
  currency: Currency
  postings: Posting[] // >= 2 legs
}

export async function postTransaction(
  input: PostTransactionInput,
  client: PoolClient
): Promise<{ globalSeq: string; eventId: string }> {
  // First, before anything is checked. A retry must return what the original
  // returned — re-running the overdraft guard against funds the original already
  // spent turns a successful retry into "Insufficient funds", which is precisely
  // the failure 1.4 removed.
  const replay = await findByIdempotencyKey(input.idempotencyKey, client)
  if (replay) {
    return { eventId: replay.eventId, globalSeq: replay.globalSeq }
  }

  const legs: Leg[] = input.postings.map((p) => ({
    ...p,
    currency: input.currency,
  }))

  const sorted = await assertPostable(legs, client)

  // The input still takes one currency for the transaction: v2 changed how the
  // event is written, not what the ledger permits. Cross-currency is the saga's
  // problem, behind a quote.

  const payload: TransactionPostedPayloadV2 = {
    idempotencyKey: input.idempotencyKey,
    postings: toPostings(sorted),
  }

  const appended = await appendEvent(
    "TransactionPosted",
    LATEST_EVENT_VERSION.TransactionPosted,
    payload,
    input.idempotencyKey,
    client
  )

  // Still reachable: two concurrent callers both found no row above, both passed
  // the guard, and one lost the ON CONFLICT race. The loser must not apply
  // balances a second time.
  if (appended.replayed) {
    return { eventId: appended.eventId, globalSeq: appended.globalSeq }
  }

  await applyToAccountBalances(payload.postings, client)

  return { eventId: appended.eventId, globalSeq: appended.globalSeq }
}
