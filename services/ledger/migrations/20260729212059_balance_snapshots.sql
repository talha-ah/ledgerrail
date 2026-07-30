-- Migration: balance_snapshots
-- Created at: Wed, 29 Jul 2026 21:20:59 GMT

-- Write your forward-only DDL here
CREATE TABLE balance_snapshots (
  account_id INT REFERENCES accounts(id),
  amount BIGINT NOT NULL DEFAULT 0,
  as_of_event_seq BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (account_id, as_of_event_seq)
)