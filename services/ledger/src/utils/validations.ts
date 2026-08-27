import type { EventEnvelope } from "../kafka/index.js"

export function validateEvent(event: unknown): event is EventEnvelope {
  if (typeof event !== "object" || event === null) return false

  const e = event as EventEnvelope

  return (
    Boolean(e.payload) &&
    typeof e.event_id === "string" &&
    typeof e.global_seq === "string" &&
    typeof e.created_at === "string" &&
    typeof e.event_version === "number" &&
    // Not a known type, just a type. Which types exist is upcast's business —
    // duplicating that list here is how the gate and the upcaster drift apart.
    typeof e.event_type === "string"
  )
}
