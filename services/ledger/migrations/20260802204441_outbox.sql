-- Migration: outbox
-- Created at: Sun, 02 Aug 2026 20:44:41 GMT

-- Write your forward-only DDL here

CREATE TABLE outbox (
  event_id UUID PRIMARY KEY REFERENCES events(event_id), -- Unique ID for outbox and reference to events
  global_seq BIGINT NOT NULL, -- copied here so the publisher can find and order its work without joining to events.
  topic TEXT NOT NULL, --  stored rather than derived, so the publisher is dumb: read row, send row. All the domain thinking happens once, at write time.
  partition_key TEXT NOT NULL, -- something like "transaction:<event_id>" e.g. "transaction:12ksad323" OR "account:7" OR "customer:10"
  published_at TIMESTAMPTZ, -- nullable gives you both "is it sent?" and "how long has it been stuck?" — a boolean throws the second one away.
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- it only contains unpublished rows, so it stays tiny no matter how big the table gets, and it's exactly the query the publisher runs.
CREATE INDEX outbox_unpublished
  ON outbox (global_seq) WHERE published_at is NULL;
