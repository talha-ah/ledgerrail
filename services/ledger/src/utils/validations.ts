import type { EventEnvelope } from "../kafka/index.js"

export function validateEvent(event: unknown): event is EventEnvelope {
  if (typeof event !== "object" || event === null) return false

  const e = event as EventEnvelope

  return (
    Boolean(e.payload) &&
    e.event_version === 1 &&
    typeof e.event_id === "string" &&
    typeof e.global_seq === "string" &&
    typeof e.created_at === "string" &&
    e.event_type === "TransactionPosted"
  )
}
