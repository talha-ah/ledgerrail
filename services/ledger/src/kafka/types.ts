import type { Event, TransactionPostedPayload } from "../types.js"

export type EventEnvelope = Omit<
  Event,
  "global_seq" | "created_at" | "payload" | "idempotency_key"
> & {
  global_seq: string
  created_at: string
  payload: TransactionPostedPayload
}
