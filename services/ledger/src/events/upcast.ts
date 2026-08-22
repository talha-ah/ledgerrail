import { assertNever, PermanentError } from "../utils/error.js"
import type {
  LatestPayload,
  VersionPayload,
  TransactionPostedPayloadV1,
  TransactionPostedPayloadV2,
} from "../types.js"

export const LATEST_EVENT_VERSION = 2

/**
 * v1 carried one currency for the whole transaction, and the command has always
 * rejected legs whose account currency differed from it — so copying it onto
 * every leg reproduces exactly what the event meant when it was written.
 * Nothing is invented, which is the property that makes an upcast safe to run
 * over history that can never be corrected.
 */
function v1ToV2(
  payload: TransactionPostedPayloadV1
): TransactionPostedPayloadV2 {
  return {
    idempotencyKey: payload.idempotencyKey,
    postings: payload.postings.map((leg) => ({
      amount: leg.amount,
      accountId: leg.accountId,
      currency: payload.currency,
    })),
  }
}

/**
 * Unknown to known. The only place in the system that decides which versions
 * exist — and the cast is the trust boundary: the envelope has been validated,
 * the payload's interior has not. Per-version payload validation is the
 * upgrade path from here, and is most of what a schema registry sells you.
 */
function asVersioned(event: {
  event_version: number
  payload: unknown
}): VersionPayload {
  switch (event.event_version) {
    case 1:
      return {
        event_version: 1,
        payload: event.payload as TransactionPostedPayloadV1,
      }
    case 2:
      return {
        event_version: 2,
        payload: event.payload as TransactionPostedPayloadV2,
      }
    default:
      // Not corruption — a message from the future, produced by a newer deploy.
      // Permanent for this build, which is precisely what a dead letter plus a
      // redrive after the upgrade is for.
      throw new PermanentError(`unknown event version: ${event.event_version}`)
  }
}

export function upcast(event: {
  event_version: number
  payload: unknown
}): LatestPayload {
  const versioned = asVersioned(event)

  switch (versioned.event_version) {
    case 1:
      return v1ToV2(versioned.payload)
    case 2:
      return versioned.payload
    default:
      return assertNever(versioned)
  }
}
