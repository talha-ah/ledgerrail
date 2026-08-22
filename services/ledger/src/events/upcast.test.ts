import { describe, it, expect } from "vitest"

import { upcast } from "./upcast.js"
import { PermanentError } from "../utils/error.js"
import { randomString } from "../utils/helpers.js"
import type {
  TransactionPostedPayloadV1,
  TransactionPostedPayloadV2,
} from "../types.js"

describe("upcast", () => {
  const idempotencyKey = randomString()

  const v1: TransactionPostedPayloadV1 = {
    idempotencyKey,
    currency: "PKR",
    postings: [
      { accountId: 1, amount: "-100" },
      { accountId: 2, amount: "100" },
    ],
  }

  const v2: TransactionPostedPayloadV2 = {
    idempotencyKey,
    postings: [
      { accountId: 1, amount: "-100", currency: "PKR" },
      { accountId: 2, amount: "100", currency: "PKR" },
    ],
  }

  it("gives every v1 leg the transaction's currency", () => {
    const out = upcast({ event_version: 1, payload: v1 })

    expect(out).toEqual(v2)
  })

  it("loses nothing a v1 event carried", () => {
    const out = upcast({ event_version: 1, payload: v1 })

    // The amounts and accounts survive untouched — the upcast adds, it does not
    // reinterpret. If this ever needs a conversion, the version is lying.
    expect(out.postings.map((l) => [l.accountId, l.amount])).toEqual(
      v1.postings.map((l) => [l.accountId, l.amount])
    )
  })

  it("leaves a v2 payload alone", () => {
    expect(upcast({ event_version: 2, payload: v2 })).toBe(v2)
  })

  it("refuses a version it has never heard of", () => {
    // A message from a newer deploy. Permanent for this build, so it dead-letters
    // and a redrive picks it up after the upgrade.
    expect(() => upcast({ event_version: 10, payload: {} })).toThrow(
      PermanentError
    )
  })
})
