import type { Event } from "../types.js"

export type EventEnvelope = Pick<
  Event,
  "event_id" | "event_type" | "event_version" | "payload"
>
