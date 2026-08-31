import type { PaymentState } from "../types.js"

/**
 * The transition table, written once so the machine can be read rather than
 * inferred from scattered UPDATEs.
 *
 *   initiated → held → quoted → settling → settled → completed
 *                 ↘        ↘         ↘        ↘
 *                        compensating → compensated / failed
 */
export const TRANSITIONS: Record<PaymentState, readonly PaymentState[]> = {
  initiated: ["held", "failed"],
  held: ["quoted", "compensating"],
  quoted: ["settling", "compensating"],

  // Not to `failed` directly: a settlement that definitively did not happen
  // still leaves a hold to return, and skipping compensation strands a
  // customer's money.
  settling: ["settled", "compensating"],

  // The bank has paid. Compensating from here means sending money back, which
  // is a new payment rather than an undo — 3.4's harder case.
  settled: ["completed", "compensating"],

  completed: [],
  compensating: ["compensated", "failed"],
  compensated: [],
  failed: [],
}

export function canTransition(from: PaymentState, to: PaymentState): boolean {
  return TRANSITIONS[from].includes(to)
}

/** Per step, not per attempt — a retry must present the same key to the rail. */
export const stepKey = (paymentId: string, step: string) =>
  `${paymentId}:${step}`
