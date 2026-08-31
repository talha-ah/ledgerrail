import type { Currency } from "@ledgerrail/money"

export interface Account {
  id: number
  type: "system" | "asset"
  currency: Currency
  account_number: string
  created_at: Date
  updated_at: Date
}

export interface Balance {
  account_id: number
  amount: bigint
  last_event_seq: string
  created_at: Date
  updated_at: Date
}

export interface AccountBalances {
  account_id: number
  posted: bigint
  pending: bigint
  updated_at: Date
}

export interface BalanceSnapshot {
  account_id: number
  amount: string
  as_of_event_seq: string
  created_at: Date
}

export interface Posting {
  accountId: number
  amount: string // strings on the wire
  currency: Currency
}

export interface TransactionPostedPayloadV1 {
  currency: Currency
  idempotencyKey: string
  postings: { accountId: number; amount: string }[] // strings on the wire
}

export interface TransactionPostedPayloadV2 {
  idempotencyKey: string
  postings: Posting[]
}

export interface FundsHeldPayloadV1 {
  holdId: string
  idempotencyKey: string
  expiresAt: string
  postings: Posting[] // customer -> hold funds
}

export interface HoldCapturedPayloadV1 {
  holdId: string
  idempotencyKey: string
  postings: Posting[] // held funds -> destination
}

export interface HoldReleasedPayloadV1 {
  holdId: string
  idempotencyKey: string
  postings: Posting[] // held funds -> customer
}

/**
 * Discriminated on BOTH axes now. Every payload carries `postings`, which is what
 * keeps the read-side projector type-agnostic: to `balances`, every event is legs
 * that sum to zero. The type matters to the write side and to the audit trail —
 * "why did this money move?" — not to the arithmetic.
 */
export type VersionPayload =
  | {
      event_type: "TransactionPosted"
      event_version: 1
      payload: TransactionPostedPayloadV1
    }
  | {
      event_type: "TransactionPosted"
      event_version: 2
      payload: TransactionPostedPayloadV2
    }
  | {
      event_type: "FundsHeld"
      event_version: 1
      payload: FundsHeldPayloadV1
    }
  | {
      event_type: "HoldCaptured"
      event_version: 1
      payload: HoldCapturedPayloadV1
    }
  | {
      event_type: "HoldReleased"
      event_version: 1
      payload: HoldReleasedPayloadV1
    }

export type EventType = VersionPayload["event_type"]

export type EventVersion = VersionPayload["event_version"]

export type LedgerEvent =
  | {
      event_type: "TransactionPosted"
      payload: TransactionPostedPayloadV2
    }
  | { event_type: "FundsHeld"; payload: FundsHeldPayloadV1 }
  | { event_type: "HoldCaptured"; payload: HoldCapturedPayloadV1 }
  | { event_type: "HoldReleased"; payload: HoldReleasedPayloadV1 }

export interface Event {
  global_seq: bigint
  event_id: string
  // A plain string, like the column, for the same reason event_version is a
  // number: the journal can hold a type this build has never heard of.
  event_type: string
  // A plain number, like the column. Nothing in the schema constrains it, and
  // upcast is the single place that decides which versions exist — a second
  // declaration of that list is a second thing to keep in sync.
  event_version: number
  // Deliberately not correlated with event_version: this row came out of a
  // database, which cannot prove the two agree. Asserting that correlation is
  // exactly what upcast does, in one place, where it can be tested.
  payload: unknown
  idempotency_key: string
  created_at: Date
}

export interface Hold {
  hold_id: string
  account_id: number
  hold_account_id: number
  amount: bigint
  currency: Currency
  status: "held" | "captured" | "released"
  held_event_id: string
  settled_event_id: string | null
  expires_at: Date
  created_at: Date
  updated_at: Date
}

export interface Outbox {
  event_id: string
  global_seq: bigint
  topic: string
  partition_key: string
  published_at?: Date
  created_at: Date
}

export type PaymentState =
  | "initiated"
  | "held"
  | "quoted"
  | "settling"
  | "settled"
  | "completed"
  | "compensating"
  | "compensated"
  | "failed"

export type PaymentStepName = "quote" | "settle" | "reverse"
export type StepStatus = "started" | "succeeded" | "failed"

export interface Payment {
  payment_id: string
  idempotency_key: string
  state: PaymentState
  source_account_id: number
  destination_reference: string
  amount: bigint
  currency: Currency
  target_currency: Currency
  hold_id: string | null
  quote_rate: string | null // NUMERIC arrives as a string, deliberately
  quote_expires_at: Date | null
  attempts: number
  next_attempt_at: Date
  last_error: string | null
  request_fingerprint: string
  created_at: Date
  updated_at: Date
}

export interface PaymentStep {
  payment_id: string
  step: PaymentStepName
  attempt: number
  idempotency_key: string
  status: StepStatus
  request: unknown
  response: unknown
  error: string | null
  started_at: Date
  finished_at: Date | null
}
