-- Migration: event_store
-- Created at: Tue, 21 Jul 2026 23:57:39 GMT

-- Write your forward-only DDL here

-- Accounts

CREATE TABLE accounts (
  id SERIAL PRIMARY KEY,
  account_number TEXT UNIQUE NOT NULL,
  currency CHAR(3) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Events

CREATE TABLE events (
  global_seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
  event_type TEXT NOT NULL,
  event_version INT NOT NULL DEFAULT 1,
  payload JSONB NOT NULL,
  idempotency_key TEXT UNIQUE NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Balances

CREATE TABLE balances (
  account_id INT PRIMARY KEY REFERENCES accounts(id),
  amount BIGINT NOT NULL DEFAULT 0, -- We might not expect huge numbers for normal currencies but for low value currencies like IRAN, we can expect huge numbers
  last_event_seq BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Immutability Trigger

CREATE OR REPLACE FUNCTION events_immutability_function ()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'Append-only Table: % operations are strictly prohibited. ', TG_OP;
END;
$$;

CREATE TRIGGER events_immutability_trigger
  BEFORE UPDATE OR DELETE OR TRUNCATE ON events
  FOR EACH STATEMENT
  EXECUTE FUNCTION events_immutability_function();
