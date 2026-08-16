-- Migration: inbox
-- Created at: Sun, 16 Aug 2026 17:34:27 GMT

-- Write your forward-only DDL here

-- Records which events a consumer group has already applied, so at-least-once
-- redelivery cannot double-apply. The row is written in the SAME transaction as
-- the projection update — mark-and-apply commit together or not at all.
CREATE TABLE inbox (
  consumer_group TEXT NOT NULL,
  event_id UUID NOT NULL REFERENCES events(event_id),
  partition INT NOT NULL,
  kafka_offset BIGINT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (consumer_group, event_id)
);

-- supports pruning rows older than the topic's retention
CREATE INDEX inbox_processed_at ON inbox (processed_at);