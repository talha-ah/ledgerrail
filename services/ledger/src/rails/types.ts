import type { Currency } from "@ledgerrail/money"

/** Minor units as a string. Money crosses a wire the same
 * way it does in the journal */
export type Amount = string

export interface QuoteRequest {
  idempotencyKey: string
  from: Currency
  to: Currency
  amount: Amount
}

export interface QuoteResponse {
  quoteId: string
  /**
   * Shaped for NUMERIC(20,10), and a string for the same reason the column is:
   * a rate is not money, but it multiplies money, so it never touches a float.
   */
  rate: string
  targetAmount: Amount
  expiresAt: string
}

export interface SettleRequest {
  idempotencyKey: string
  quoteId: string
  destinationReference: string
  amount: Amount
  currency: Currency
  targetCurrency: Currency
}

export interface SettleResponse {
  railReference: string
  status: "settled"
}

/**
 * What the orchestrator is allowed to know about a bank.
 *
 * The port exists before the process does (3.0) because the orchestrator must
 * not be able to tell which side of it it is on — an in-memory fake and an HTTP
 * client have to be substitutable, or the tests prove something the production
 * path does not do.
 */
export interface Rail {
  quote(req: QuoteRequest): Promise<QuoteResponse>

  /**
   * Idempotent on `idempotencyKey`. This is not a nicety, it is the entire
   * safety argument: 3.2 commits the key before the call precisely so a retry
   * can present the same one, and if the rail does not honour it, every retry
   * in this system is a second payment (ADR 0011).
   */
  settle(req: SettleRequest): Promise<SettleResponse>

  /**
   * "What do you think happened with key K?" Deliberately unused in 3.3 — it is
   * the question 3.5 exists to ask, and a port that cannot ask it is a port 3.5
   * would have to change. Declaring it now costs nothing and records the
   * requirement where the requirement lives.
   */
  lookup(idempotencyKey: string): Promise<SettleResponse | null>
}
