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

export interface TransactionPostedPayloadV1 {
  currency: Currency
  idempotencyKey: string
  postings: { accountId: number; amount: string }[] // strings on the wire
}

export interface TransactionPostedPayloadV2 {
  idempotencyKey: string
  postings: { accountId: number; amount: string; currency: Currency }[] // strings on the wire
}

/** Discriminated on event_version, so a switch over it can be exhaustive. */
export type VersionPayload =
  | { event_version: 1; payload: TransactionPostedPayloadV1 }
  | { event_version: 2; payload: TransactionPostedPayloadV2 }

export type EventVersion = VersionPayload["event_version"]

export type LatestPayload = TransactionPostedPayloadV2

export interface Event {
  global_seq: bigint
  event_id: string
  event_type: "TransactionPosted"
  // A plain number, like the column. Nothing in the schema constrains it, and
  // upcast is the single place that decides which versions exist — a second
  // declaration of that list is a second thing to keep in sync.
  event_version: number
  // Deliberately not correlated with event_version: this row came out of a
  // database, which cannot prove the two agree. Asserting that correlation is
  // exactly what upcast does, in one place, where it can be tested.
  payload: TransactionPostedPayloadV1 | TransactionPostedPayloadV2
  idempotency_key: string
  created_at: Date
}

export interface Outbox {
  event_id: string
  global_seq: bigint
  topic: string
  partition_key: string
  published_at?: Date
  created_at: Date
}
