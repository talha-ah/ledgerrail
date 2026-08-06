-- Migration: account_balances
-- Created at: Thu, 06 Aug 2026 16:04:10 GMT

-- Write your forward-only DDL here
CREATE TABLE account_balances (
  account_id INT PRIMARY KEY REFERENCES accounts(id),
  posted BIGINT NOT NULL DEFAULT 0,
  pending BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)