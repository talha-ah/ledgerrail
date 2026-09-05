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

  // Back to `held` is a re-pricing, not a rollback: the funds are still
  // reserved, only the price went stale. It has to be taken *before* `settling`
  // is entered — once it is, the settle may already have reached the bank and
  // the only way out is forward, which is why `settling` has no edge back here.
  quoted: ["settling", "held", "compensating"],

  settling: ["settled", "compensating"],
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
