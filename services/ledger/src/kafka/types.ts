import type { Event } from "../types.js"

export type EventEnvelope = Omit<
  Event,
  "event_version" | "global_seq" | "created_at" | "payload" | "idempotency_key"
> & {
  // Omitted above so this replaces it rather than intersecting with it —
  // (1 | 2) & number is 1 | 2, which is how the wire type quietly became a
  // promise the wire cannot keep. A consumer can be handed any version.
  event_version: number
  global_seq: string
  created_at: string
  payload: unknown
}
