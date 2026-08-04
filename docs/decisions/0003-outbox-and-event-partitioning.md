# 0003 — Transactional outbox and event partitioning

**Status:** Accepted
**Date:** 2026-08-02
**Stage:** 2.1

## Context

In Stage 1 the balance projection was updated synchronously by `postTransaction`, in the
same database transaction as the event append. That is atomic because both writes land in
one Postgres commit.

Stage 2 moves the projection out of process: events are published to Kafka and a consumer
updates `balances`. The command must therefore do two things — persist the event to
Postgres *and* publish it to Kafka — and those are two separate systems that cannot commit
together.

This is the **dual-write problem**, and no ordering of the two operations is safe:

- Commit to Postgres, then publish → crash in between: the event exists and nobody hears
  about it. Projections silently diverge from the journal.
- Publish, then commit → crash in between: consumers act on an event that was never
  persisted. Money that does not exist has been announced to the world.

## Decision 1 — Transactional outbox

The command inserts a row into an `outbox` table **in the same transaction as the event**.
There is no second system involved at write time, so the gap disappears: one commit covers
both the event and the recorded intent to publish it. A separate relay process later reads
unpublished rows, produces to Kafka, and marks them published.

The relay may crash after producing but before marking, so delivery is **at-least-once**.
That is accepted deliberately; see Decision 4 and Consequences.

## Decision 2 — Reference the event, do not copy the payload

The outbox row holds `event_id` (a foreign key to `events`) rather than a copy of the
payload. The relay joins to `events` to get the bytes to publish.

The usual objection — "the source row might change between recording the intent and
publishing" — does not apply here, because `events` is append-only and enforced immutable
by a database trigger (see [0001]). The Stage 1 invariant makes a normally-risky
denormalisation choice safe. It also keeps the outbox small and guarantees that what is
published is byte-identical to what was stored.

`global_seq` is denormalised into the outbox anyway, so the relay can find and order its
work without a join.

## Decision 3 — `published_at TIMESTAMPTZ NULL` rather than a boolean

`NULL` means unpublished. This carries strictly more information than a boolean: it answers
both *"has this been sent?"* and *"how long has this row been stuck?"*. The second question
is the one that matters at 3am, and a boolean throws it away.

A partial index — `ON outbox (global_seq) WHERE published_at IS NULL` — makes the relay's
query cheap and stays small forever, because rows drop out of the index once published.

## Decision 4 — One message per transaction, keyed by `transaction:<event_id>`

The original plan was to partition by account id to get per-account ordering. That was
reconsidered, and rejected, for the following reasons.

**Ordering exists to protect something. We checked what.** A partition key buys ordering
within a partition, at the cost of parallelism. So the question is what actually breaks if
two events for the same account are applied out of order:

- `amount = amount + delta` is commutative — order is irrelevant to the final balance.
- `last_event_seq` *is* order-sensitive: applied out of order it ends up holding a lower
  value than the highest event applied.

We then checked what reads `last_event_seq`, and the answer is **nothing that matters**.
`rebuildBalances` replays from the `events` table in Postgres in `global_seq` order, which
Kafka delivery cannot disturb, and `balancesSnapshot` takes its cursor from
`MAX(global_seq)` on `events`, not from the projection. `balances.last_event_seq` is a
diagnostic field, not a correctness one.

So per-account ordering would have cost real complexity and protected nothing.

**Keeping the transaction whole is worth more.** The alternative — one message per posting,
keyed by account — genuinely delivers per-account ordering, but no downstream consumer can
then see a complete transfer. A notification consumer would see "50 left account 4" with no
idea where it went, and the Σ = 0 invariant becomes unverifiable from the stream. One
message per transaction keeps the atomic unit atomic.

**Ordering would not have solved the actual risk.** At-least-once delivery means duplicates
are guaranteed, and `+=` applied twice is wrong money. No partition key prevents that; only
an idempotent consumer does. Leaning on ordering would have been a crutch hiding the problem
that actually has to be solved.

**Keys are namespaced** (`transaction:<event_id>`, not a bare uuid). This pays immediately
rather than speculatively: the routing basis is legible when reading a topic, unrelated
entity types with colliding numeric ids cannot hash to the same key, and a future change of
keying strategy is visible in the data itself. Key construction lives in one helper —
a mismatch such as `txn:` vs `transaction:` would silently route to a different partition
and quietly void whatever ordering was assumed, with no error.

## Decision 5 — The relay produces first, then marks published

The relay does two things per row: produce the message to Kafka, and set `published_at` in
Postgres. These are two systems, so they cannot commit together — the same constraint that
motivated the outbox in the first place, reappearing one layer down.

There is no ordering of those two operations that avoids **both** loss and duplication:

- **Mark first, then produce.** A crash in the gap leaves a row marked published that was
  never sent. The relay will never look at it again, so the event is lost permanently and
  silently — the projection simply never learns about it. Unrecoverable without a manual
  audit comparing `events` to the topic.
- **Produce first, then mark.** A crash in the gap leaves a published message and an
  unmarked row. The relay republishes it on the next run, so the message is delivered twice.
  Recoverable, because a duplicate can be detected and discarded downstream.

We produce first. Delivery is therefore **at-least-once**, and duplicates are guaranteed to
happen eventually rather than being a rare edge case.

The reasoning is that these two failure modes are not symmetric. A lost event is
unrecoverable and invisible; a duplicate event is recoverable and detectable. Given a choice
between a failure the system can absorb and one it cannot, we take the absorbable one.

This is the entire justification for the inbox in 2.4: it is not defensive programming
against an unlikely event, it is the required counterpart to a deliberate choice made here.
Note also that `applyToBalances` uses `amount = amount + delta`, which is commutative but
**not idempotent** — applying the same event twice produces a wrong balance. Duplicate
suppression is therefore load-bearing for correctness, not merely tidy.

Wrapping produce-and-mark in a database transaction was considered and rejected: Postgres
cannot roll back a Kafka send, so the transaction would imply an atomicity that does not
exist while adding real overhead. A single `UPDATE` is already atomic.

## Consequences

- **Consumers must be idempotent.** At-least-once delivery is a deliberate choice, so
  duplicate suppression moves into our own inbox/dedup table rather than relying on broker
  machinery. This is the next checkpoint.
- **Events for one account may be processed out of order.** Today this only makes
  `balances.last_event_seq` untidy. If a consumer that genuinely needs per-account ordering
  is added — statement generation, fraud velocity — the migration is to publish one message
  per posting keyed by `account:<id>`. Note that this also changes the outbox's shape:
  one event would produce N rows, so `event_id` could no longer be its primary key.
- **The overdraft check now reads a lagging read model.** `postTransaction` enforces the
  balance floor with `SELECT … FOR UPDATE` against `balances`, which becomes an
  asynchronously updated projection. This is the central tension of CQRS — an invariant
  cannot be enforced against an eventually-consistent read model — and it is unresolved as
  of this decision. It must be addressed before the synchronous projection is removed.

## Alternatives considered

- **Publish directly from the command, no outbox.** Rejected: this is the dual-write problem
  with no mitigation.
- **Poll the `events` table directly instead of an outbox table.** Tempting, since `events`
  is already an ordered append-only log. Rejected because `global_seq` is assigned at insert
  time, not commit time: a transaction can take a lower sequence number and commit *after*
  one that took a higher number, so a relay tracking a sequence cursor could advance past an
  event that had not yet committed and skip it permanently. An explicit `published_at` flag
  claims rows by state rather than by position, which does not have this failure mode.
- **Key by `event_type`.** Rejected: every message would share one key, hence one partition —
  total ordering with zero parallelism, and no way to scale consumers.
- **Key by `global_seq` or another unique value.** Equivalent to random distribution: maximum
  parallelism, no ordering guarantee between any two messages whatsoever.
