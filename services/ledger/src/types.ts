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
  amount: string
  last_event_seq: string
  created_at: Date
  updated_at: Date
}

export interface Event {
  global_seq: string
  event_id: string
  event_type: string
  event_version: number
  payload: object
  idempotency_key: string
  created_at: Date
}
