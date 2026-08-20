# 0006 — Dead letters in Postgres, for permanent failures only

**Status:** Accepted
**Date:** 2026-08-20
**Stage:** 2.5

## Context

Throwing inside `eachMessage` does not skip a message: the offset is never committed,
so kafkajs redelivers it forever and the partition stops. That is correct for a
database outage and catastrophic for a malformed payload, and until now the consumer
did neither well — it threw on some failures and silently `return`ed on others,
which committed the offset and lost the message with nothing but a log line.

There are only three things to do with a message that fails: throw and block, skip
silently and lose it, or skip loudly and record it somewhere it can be inspected.
The third one is a dead letter queue.

## Decision 1 — The queue is a Postgres table, not a Kafka topic

A `ledger.events.dlq` topic is the conventional answer. It was rejected because
publishing to it and then committing the offset is the dual-write problem again, and
this time an at-least-once repeat is not absorbable: a re-produced dead letter is a
second copy of the message with no key to deduplicate it against.

A row keyed by delivery position is idempotent under exactly the same redelivery —
recording it twice is one row. The table also sits in the database that already holds
the inbox and the projection, so a redrive is a query rather than another consumer,
and an operator has one place to look rather than two.

**The cost accepted:** failures are not consumable by other services, and none of
Kafka's tooling applies to them. If another service ever needs them, the outbox
pattern already in this repo relays the table to a topic without changing the
recording path.

## Decision 2 — Keyed by `(consumer_group, topic, partition, kafka_offset)`

Delivery position, not event identity. A message that fails to parse may carry no
`event_id` — or one that is not a UUID, which is why that column is `TEXT` with no
foreign key to `events`. The same reasoning makes `raw` a `BYTEA`: a Kafka value is
bytes, and `TEXT` would reject a payload containing a null byte.

This is the inverse of the choice in [0005](0005-consumer-inbox.md), where
`inbox.event_id` keeps its foreign key deliberately. An inbox row asserts that an
event was applied; a dead letter row asserts nothing about the message except that
it arrived and could not be. **A quarantine table that can reject its input cannot
do its job**, because the rejection happens inside the recovery path — leaving no
record and a blocked partition, which is strictly worse than the failure it was
handling.

## Decision 3 — Only permanent failures are dead-lettered

There is no retry budget. A failure is classified permanent — an explicit
`PermanentError`, or a Postgres SQLSTATE in class 22 (data exception) or 23
(integrity violation) — or it is rethrown and the partition blocks until the
dependency recovers.

A bounded budget was rejected because it inverts the risk. A thirty-second database
outage would drain the topic into this table at full throughput, turning a recoverable
latency problem into permanent, hand-sorted data loss. Everything unrecognised is
therefore treated as transient: a message wrongly retried costs latency, a message
wrongly quarantined costs correctness.

**Revisit when** a failure that classifies as transient recurs indefinitely — a lock
timeout on a hot account is the likely one. That needs a bounded attempt count, and
it must be a column rather than a counter in memory, since a crash-looping consumer
resets an in-memory budget on every restart and would never reach it.

## Decision 4 — A dead letter does not claim the inbox

The two tables stay disjoint: `inbox` means applied, `dead_letters` means not applied
and needs a person. Writing both would be atomic and convenient, and it would make
redrive impossible — the claim would already be there, so the redriven event would be
skipped as a duplicate and silently never applied.

Redrive runs the message back through the same handler, so it takes the inbox claim
then. That is also what makes redriving twice safe, and what stops the redrive from
becoming a second writer of the projection, which [0004](0004-write-side-balances.md)
established that `balances` cannot have.

## Consequences

- **Head-of-line blocking is removed for permanent failures only.** A transient
  failure still blocks its partition, by design. Lag on a stuck partition is the
  signal that a dependency is down, not that a message is bad.
- **A redrive applies events out of order**, since newer messages were processed
  while the poison one sat in the table. Balances survive this because applying
  deltas is commutative, but `last_event_seq` must move with `GREATEST` — a cursor
  that walks backwards claims the projection is behind ground it has already covered.
- **Rows are resolved, not deleted.** How often a message dead-letters and how long
  it sat there is the operational signal. Unlike events (immutable), projections
  (rebuildable) or snapshots (disposable), this row is the _only_ copy of something
  the system could not process — it falls into none of the three durability
  categories and cannot be reconstructed from anything else.
- **The table therefore cannot be pruned by Kafka retention** the way the inbox can.
  Only resolved rows are safe to delete, and only after a window. It also stores raw
  message payloads, so in a payments system it holds customer data and inherits a
  retention obligation the inbox does not have.
- **The redrive is a one-shot batch, not a loop in the consumer.** "Try the broken
  thing again" is an operational decision that follows a fix, so it exits non-zero
  when the run itself fails and reports what is still broken when the messages are.

## Related

- [0003](0003-outbox-and-event-partitioning.md) — chose at-least-once delivery, which
  is why re-recording a dead letter has to be idempotent.
- [0005](0005-consumer-inbox.md) — the inbox this deliberately does not write to, and
  the foreign-key decision this one inverts.
