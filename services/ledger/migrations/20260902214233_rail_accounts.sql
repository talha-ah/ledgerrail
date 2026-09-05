-- Migration: rail_accounts
-- Created at: Wed, 02 Sep 2026 21:42:33 GMT

-- One clearing account per currency: money that has left the customer and been
-- handed to the rail, but has not yet reached a destination this ledger can
-- name. The mirror of HELD:{ccy} on the far side of a settlement — a hold is
-- money we have not sent, this is money we can no longer reach.
--
-- `system` for exactly the reason the reserve is: the guard skips system
-- accounts, so no floor is locked and every payment in a currency does not
-- serialise on one row (ADR 0002, ADR 0010). Nothing new is asked of this
-- balance — the once-only invariant still lives on the hold row.
INSERT  INTO accounts ( account_number, currency, type)
VALUES
('RAIL:PKR', 'PKR', 'system'),
('RAIL:USD', 'USD', 'system'),
('RAIL:EUR', 'EUR', 'system'),
('RAIL:GBP', 'GBP', 'system'),
('RAIL:JPY', 'JPY', 'system');

ALTER TABLE payments ADD COLUMN quote_id TEXT;

COMMENT ON COLUMN payments.quote_id IS
  'The rail''s id for the price this payment settles at. On the payment rather '
  'than dug out of a step''s JSONB response, so the settle request is a pure '
  'function of this row — which is what lets a retry resend an identical '
  'request and a reconciler reconstruct one from the row alone.';

ALTER TABLE payments ADD COLUMN lease_token UUID;

COMMENT ON COLUMN payments.lease_token IS
  'Who currently holds this payment. next_attempt_at answers *when* it may be '
  'retried, which is a different question — two workers can hold the same '
  'deadline, and a clock is not an identity. Also not a timestamp for a duller '
  'reason: TIMESTAMPTZ stores microseconds, a JS Date carries milliseconds, and '
  'an equality fence between them matches nothing.';