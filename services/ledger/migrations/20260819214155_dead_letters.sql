-- Migration: dead_letters
-- Created at: Wed, 19 Aug 2026 21:41:55 GMT

-- Write your forward-only DDL here

-- Messages this consumer group could not process and never will. Recording one
-- lets the offset advance, so a poison message stops blocking everything behind
-- it — the difference between skipping loudly and skipping silently.
--
-- This is a quarantine table: it holds untrusted, already-proven-invalid data.
-- That shapes every column below. Anything strict enough to reject bad input
-- would reject the very messages the table exists to capture.

CREATE TABLE dead_letters (
  consumer_group TEXT NOT NULL,
  topic TEXT NOT NULL,
  partition INT NOT NULL,
  kafka_offset BIGINT NOT NULL,

  -- No foreign key to events
  -- event_id as text because invalid events can pass strings and it's not always UUID
  event_id TEXT,
  message_key TEXT,

  -- Exactly what came off the topic, untruncated. BYTEA, not TEXT, because a
  -- Kafka value is bytes: TEXT rejects null bytes, so a binary payload could not
  -- be stored. This is the only surviving copy once the offset moves on.
  raw BYTEA,

  error TEXT NOT NULL,
  error_code TEXT, -- SQLSTATE, when the failure came from Postgres

  failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at TIMESTAMPTZ,

  -- Delivery position, not event identity: the only key every message has.
  -- A malformed message may carry no event_id, and re-delivering the same
  -- message must not create a second row.
  PRIMARY KEY (consumer_group, topic, partition, kafka_offset)
);

-- The operational query is "what is quarantined right now", so index only those.
CREATE INDEX dead_letters_unresolved 
  ON dead_letters (failed_at)
  WHERE resolved_at IS NULL;