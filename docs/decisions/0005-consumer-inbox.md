# 0005 — Consumer idempotency via an inbox

**Status:** Accepted
**Date:** 2026-08-06
**Stage:** 2.4

## Context

[0003](0003-outbox-and-event-partitioning.md) chose produce-then-mark in the relay, which
makes delivery **at-least-once**: a crash between publishing and marking the outbox row
republishes the message. Duplicates are therefore guaranteed to occur eventually, not a rare
edge case.

The balances projection applies deltas — `amount = amount + delta`. That operation is
commutative, so ordering does not matter, but it is **not idempotent**: applying the same
event twice moves the balance twice. This was observed directly during Stage 2.3, where the
read side settled at exactly 2× the write side after the same five events were applied by two
different writers.

Nothing in Kafka prevents this. Exactly-once semantics exist in the broker but only within
Kafka's own boundaries; the moment an effect lands in Postgres, the guarantee has to be ours.

## Decision 1 — An inbox table, keyed by `(consumer_group, event_id)`

Each consumer group records the events it has applied. A duplicate is detected by the primary
key rather than by a preceding read, so there is no time-of-check/time-of-use window.

The key is **not** `event_id` alone. Deduplicating globally would mean the first group to
process an event starves every other group of it — a notifications consumer must be able to
handle an event the balances consumer already handled. The dedup question is always
*"has **this consumer** already applied this event?"*

Detection uses `INSERT … ON CONFLICT DO NOTHING RETURNING`, the same form
`postTransaction` uses for idempotency keys: a duplicate yields zero rows instead of raising
`23505`, which matters because an error would abort the surrounding transaction and make the
follow-up work impossible.

## Decision 2 — The claim and the effect share one transaction

The inbox insert and the projection update commit together or not at all.

Splitting them recreates the dual-write problem one layer down:

- **Mark first, then apply** — a crash in the gap leaves the event recorded as processed but
  never applied. It will never be redelivered to this group, so the projection is permanently
  wrong and nothing indicates it.
- **Apply first, then mark** — a crash in the gap leaves the effect applied and unrecorded, so
  redelivery applies it a second time. Which is the very thing the inbox exists to prevent.

Inside a single transaction there is no gap. This is also why the inbox must live in the same
database as the projection it protects, and why an external store such as Redis cannot serve
this purpose: the dedup record has to be transactional with the effect it guards.

## Decision 3 — Read-side tables retain foreign keys into write-side tables

`balances.account_id` references `accounts(id)`, and `inbox.event_id` references
`events(event_id)`.

The alternative was to strip foreign keys from read-side tables on the grounds that a
projection is conceptually a separate service which, when split out, could not reference the
write side's tables at all.

We keep them, deliberately, because the system is a single database today and the constraints
carry real value now:

- They catch genuine bugs — a projection writing a balance for an account that does not exist
  is a defect, and the database refuses it rather than storing it.
- **They document the coupling.** When the read side is eventually split into its own
  deployable, every foreign key crossing the boundary is a piece of work that must be
  resolved, and enumerating them is a query rather than an act of memory. The database will
  not permit the split until each one has been dealt with.

### The cost accepted

A foreign key on a projection imposes an **ordering requirement across partitions**. Today
accounts are created by direct insert, so the constraint is always satisfiable. Once account
creation becomes an event, that event and a transaction event may land on different
partitions — which have no ordering relative to one another — so a consumer can legitimately
see *"transfer to account 7"* before *"account 7 created"*, and the foreign key will reject
it.

At that point the constraint would no longer be protecting against a bug (the write side has
already guaranteed the account exists); it would be rejecting a message purely because the
projection has not caught up. That is the moment to revisit this decision, and it is expected
to arrive in Stage 3 when accounts move behind the log.

## Consequences

- **The inbox grows by one row per event per consumer group.** It is bounded by Kafka's
  retention: a message older than the topic's retention cannot be redelivered, so its inbox
  row can never be needed. A periodic delete of rows older than retention plus a margin keeps
  it small. Raising retention requires raising that margin, or the window reopens.
- **Duplicate suppression is per group, so a new consumer group processes the whole retained
  log from the beginning** — which is the desired behaviour, and the same mechanism that makes
  adding a projection cheap.
- **This does not make the consumer's side effects idempotent in general** — only its database
  writes. A consumer that sends an email before committing would still send two. Effects that
  leave the transaction need their own idempotency.
- **The write side and read side remain in one database.** Splitting them later means removing
  the two foreign keys above and giving the projection its own connection; the transactional
  requirement of Decision 2 travels with the projection, not with the ledger.

## Related

- [0003](0003-outbox-and-event-partitioning.md) — chose at-least-once delivery, of which this
  is the required counterpart.
- [0004](0004-write-side-balances.md) — established that `balances` has exactly one writer;
  the inbox protects against that writer applying the same event twice, which is a different
  problem from a second writer applying it once each.
