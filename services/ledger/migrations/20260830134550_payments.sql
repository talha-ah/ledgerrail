-- Migration: payments
-- Created at: Sun, 30 Aug 2026 13:45:50 GMT

-- Write your forward-only DDL here

-- Workflow state, not money. The journal answers "where is the money"; this
-- answers "where is this payment". Retries, attempts and timeouts are
-- operational facts — put them in the journal and every replay of the ledger
-- replays your outage.
CREATE TABLE payments (
  payment_id       UUID        PRIMARY KEY,

  -- The caller's key for the whole payment, distinct from the per-step keys the
  -- rails are given below.
  idempotency_key  TEXT        NOT NULL UNIQUE,

  -- Every state names a crash window: a place a process can die where recovery
  -- differs from its neighbours. `settling` and `settled` are two states because
  -- the gap between them is the only one where we do not know what happened.
  state            TEXT        NOT NULL CHECK (state IN (
                                 'initiated', 'held', 'quoted', 'settling',
                                 'settled', 'completed',
                                 'compensating', 'compensated', 'failed')),

  source_account_id     INT     NOT NULL REFERENCES accounts (id),
  destination_reference TEXT    NOT NULL,
  amount           BIGINT      NOT NULL CHECK (amount > 0),
  currency         CHAR (3)    NOT NULL,
  target_currency  CHAR (3)    NOT NULL,

  hold_id          UUID        REFERENCES holds (hold_id),

  request_fingerprint TEXT NOT NULL,

  -- NUMERIC, never a float: a rate is not money but it multiplies money.
  -- node-postgres hands NUMERIC back as a string, which is exactly what we want.
  quote_rate       NUMERIC(20, 10),
  quote_expires_at TIMESTAMPTZ,

  -- Provisioned now so 3.3's resumer has something to drive. A state machine
  -- with no notion of "look at this again later" cannot be resumed, and adding
  -- the column once rows are in flight is worse.
  attempts         INT         NOT NULL DEFAULT 0,
  next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_error       TEXT,

  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX payments_claimable
  ON payments (next_attempt_at)
  WHERE state NOT IN ('completed', 'compensated', 'failed');

-- One row per *attempt* at a side effect, append-only in spirit: a row is
-- written with its idempotency key and committed BEFORE the call, then updated
-- with the outcome after. The row that exists with status 'started' and no
-- finished_at is the record of an attempt whose result we never learned.
CREATE TABLE payment_steps (
  payment_id       UUID        NOT NULL REFERENCES payments (payment_id),
  step             TEXT        NOT NULL CHECK (step IN ('quote', 'settle', 'reverse')),
  attempt          INT         NOT NULL,

  -- Per step, NOT per attempt: a retry must present the bank the same key, or
  -- the bank sees two payments and pays twice. Derived from (payment, step) so
  -- it is reconstructible from the payment row alone, which is the position a
  -- reconciler is in when it finds a payment stuck mid-settlement.
  idempotency_key  TEXT        NOT NULL,

  status           TEXT        NOT NULL CHECK (status IN ('started', 'succeeded', 'failed')),
  request          JSONB,
  response         JSONB,
  error            TEXT,

  started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at      TIMESTAMPTZ,

  PRIMARY KEY (payment_id, step, attempt),

  CONSTRAINT payment_steps_finished_matches_status CHECK (
    (status = 'started' AND finished_at IS NULL) OR
    (status IN ('succeeded', 'failed') AND finished_at IS NOT NULL)
  )
);

CREATE INDEX payment_steps_unfinished
  ON payment_steps (payment_id)
  WHERE status = 'started';