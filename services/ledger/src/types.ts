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

export interface BalanceSnapshot {
  account_id: number
  amount: string
  as_of_event_seq: string
  created_at: Date
}

export interface TransactionPostedPayload {
  currency: Currency
  idempotencyKey: string
  postings: { accountId: number; amount: string }[] // strings on the wire
}

export interface Event {
  global_seq: string
  event_id: string
  event_type: string
  event_version: number
  payload: TransactionPostedPayload
  idempotency_key: string
  created_at: Date
}
