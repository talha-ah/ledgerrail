-- Migration: holds
-- Created at: Wed, 26 Aug 2026 22:12:59 GMT
-- Write your forward-only DDL here
-- One reserve account per currency. Typed `system` deliberately: the guard skips
-- system accounts, so holds do not serialise on this row the way they would if a
-- non-negative floor had to be locked and checked. The invariant that a hold is
-- consumed once lives on the hold row below, which is where it belongs — it is a
-- fact about the hold's lifecycle, not about an account's balance.
INSERT  INTO accounts ( account_number, currency, type)
VALUES
('HELD:PKR', 'PKR', 'system'),
('HELD:USD', 'USD', 'system'),
('HELD:EUR', 'EUR', 'system'),
('HELD:GBP', 'GBP', 'system'),
('HELD:JPY', 'JPY', 'system');

-- Money reserved for a payment that has not happened yet. The postings live in
-- the journal like everything else; this table is the lifecycle around them.
CREATE TABLE holds (
  hold_id          UUID        PRIMARY KEY,
  account_id       INT         NOT NULL   REFERENCES accounts (id),
  hold_account_id  INT         NOT NULL   REFERENCES accounts (id),
  amount           BIGINT      NOT NULL   CHECK (amount > 0),
  currency         CHAR (3)    NOT NULL,
  
  status           TEXT        NOT NULL   CHECK (status IN ('held', 'captured', 'released')),
  
  held_event_id    UUID        NOT NULL   REFERENCES events (event_id),
  settled_event_id UUID        REFERENCES events (event_id),
  
  -- A hold with no deadline leaks a customer's money when a saga dies mid-flight,
  -- which is precisely what 3.5 arranges. The sweeper that acts on this is 3.4;
  -- the column is here so the sweeper has something to read.
  expires_at       TIMESTAMPTZ NOT NULL,
  created_at       TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  updated_at       TIMESTAMPTZ DEFAULT NOW() NOT NULL,

  -- status and settled_event_id are two encodings of the same fact, so the
  -- database keeps them agreeing: "captured with no settlement event" and
  -- "still held but pointing at one" become unrepresentable.
  CONSTRAINT holds_settlement_matches_status CHECK (
    (status = 'held' AND settled_event_id IS NULL) OR
    (status IN ('captured', 'released') AND settled_event_id IS NOT NULL)
  )
);

CREATE INDEX holds_expiring ON holds(expires_at) WHERE status = 'held';

CREATE INDEX holds_account ON holds(account_id) WHERE status = 'held';