import { assertNever, PermanentError } from "../utils/error.js"
import type {
  LedgerEvent,
  VersionPayload,
  FundsHeldPayloadV1,
  HoldCapturedPayloadV1,
  HoldReleasedPayloadV1,
  TransactionPostedPayloadV1,
  TransactionPostedPayloadV2,
} from "../types.js"

export const LATEST_EVENT_VERSION = {
  TransactionPosted: 2,
  FundsHeld: 1,
  HoldCaptured: 1,
  HoldReleased: 1,
}

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

interface WireEvent {
  event_type: string
  event_version: number
  payload: unknown
}

function asKnown(event: WireEvent): VersionPayload {
  const { event_type: type, event_version: version, payload } = event

  switch (type) {
    case "TransactionPosted":
      if (version === 1)
        return {
          event_type: type,
          event_version: version,
          payload: payload as TransactionPostedPayloadV1,
        }
      if (version === 2)
        return {
          event_type: type,
          event_version: version,
          payload: payload as TransactionPostedPayloadV2,
        }

      break
    case "FundsHeld":
      if (version === 1)
        return {
          event_type: type,
          event_version: version,
          payload: payload as FundsHeldPayloadV1,
        }
      break
    case "HoldCaptured":
      if (version === 1)
        return {
          event_type: type,
          event_version: version,
          payload: payload as HoldCapturedPayloadV1,
        }
      break
    case "HoldReleased":
      if (version === 1)
        return {
          event_type: type,
          event_version: version,
          payload: payload as HoldReleasedPayloadV1,
        }
      break
  }

  // A type or version this build has never heard of — a message from the future,
  // permanent for this code, which is what the dead letter and a redrive after
  // the upgrade are for.
  throw new PermanentError(`unknown event: ${type} v${version}`)
}

export function upcast(event: WireEvent): LedgerEvent {
  const known = asKnown(event)

  switch (known.event_type) {
    case "TransactionPosted":
      switch (known.event_version) {
        case 1:
          return {
            event_type: known.event_type,
            payload: v1ToV2(known.payload),
          }
        case 2:
          return {
            event_type: known.event_type,
            payload: known.payload,
          }
        default:
          return assertNever(known)
      }
    case "FundsHeld":
    case "HoldCaptured":
    case "HoldReleased":
      return {
        event_type: known.event_type,
        payload: known.payload,
      } as LedgerEvent
    default:
      return assertNever(known)
  }
}
