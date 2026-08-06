# 0004 — Where the balance invariant lives

**Status:** Accepted
**Date:** 2026-08-06
**Stage:** 2.3

## Context

Through Stage 1 the `balances` table was written synchronously by `postTransaction`, in the
same transaction as the event append. The overdraft guard read it with
`SELECT … FOR UPDATE`, which was sound: the number it read was authoritative, and the lock
held until the transaction that would change it committed.

Stage 2.3 moved the projection behind Kafka. `balances` is now maintained by a consumer
that runs *after* the command has returned. The guard did not move, so it began reading a
table that no longer reflects anything the current transaction has done — or, in an empty
test environment, anything at all.

The failing test made it concrete: two concurrent debits both read a balance that had not
caught up, and neither could be correctly evaluated. The same failure exists in production,
where it presents more quietly — fund an account and immediately spend it twice, and both
debits are approved against a stale balance.

This is the central tension of CQRS, and it has a general form:

> **An invariant cannot be enforced against an eventually-consistent read model.**

The read model is, by construction, a statement about the past. Enforcement requires a
statement about *now*, held still long enough to act on.

## Decision

**The write side keeps the minimum state it needs to enforce its own invariants. The read
side is a projection, and is used only for reading.**

Concretely, a second table — `account_balances` — is maintained by `postTransaction` inside
its own transaction, and is what the overdraft guard reads and locks. The `balances`
projection continues to be maintained asynchronously by the consumer and continues to serve
queries, statements and rebuilds.

Two tables, two owners, no double-counting: the command never writes `balances`, and the
consumer never writes `account_balances`.

### Available versus posted

The write-side table separates two quantities, following standard payments vocabulary:

- **`posted`** — money that has actually moved. Ledger balance.
- **`pending`** — outstanding holds; money committed but not yet settled.
- **available = posted + pending** — what may be spent right now.

This is the distinction every bank exposes: tapping a card places an *authorisation hold*
that reduces available balance immediately and synchronously, while the ledger entry itself
is posted days later at capture. The invariant is enforced against the available balance;
the ledger settles asynchronously. That is precisely the shape of the problem here.

`pending` is unused until Stage 3 introduces the payment saga, whose first step is a funds
hold. It is included now — against the general preference for building only what is needed —
for one specific reason: it changes the invariant's expression from `posted + delta >= 0` to
`posted + pending + delta >= 0`. That expression is the single riskiest line in the system,
and it is better rewritten while the ledger is empty than retrofitted once it is not.

## Consequences

- **`balances` may lag arbitrarily behind `account_balances`.** Anything user-facing that
  needs a to-the-moment figure must read the write side; anything reporting-shaped can use
  the projection. That choice must now be made deliberately at each call site, which is a
  cost of the split and not an accident.
- **Two tables can disagree** if the consumer stalls or a message is lost. Both are derived
  from `events`, so both are rebuildable, and a reconciliation check comparing them is a
  cheap and high-value monitor — a persistent divergence means the pipeline is broken.
- **`balances` now has exactly one writer: the consumer.** Rebuilding it is an *operational
  procedure* — stop the consumer, truncate the table, seek the consumer group's offsets to
  the start, restart — not a function call. Calling `rebuildBalances` against a live system
  creates a second writer with an independent cursor (snapshot sequence versus Kafka
  offset), and because the projection applies deltas rather than absolute values, both
  writers applying the same events silently doubles every balance. This was observed
  directly: the read side settled at exactly 2× the write side after a rebuild and the
  consumer each processed the same five events. Note that per-consumer deduplication does
  not help here — the duplicate is a second author, not a redelivery.
  `rebuildBalances` is retained for tests, where no consumer runs and it is therefore the
  sole writer.
- **The names are one word apart.** `account_balances` (write) and `balances` (read) are
  easy to confuse, and confusing them means enforcing an invariant against the wrong table —
  which is the exact bug this decision exists to fix. Renaming the projection is worth
  considering.
- **Lock ordering still matters.** The guard locks `account_balances` rows in ascending
  `account_id` order, as postings are sorted before use, so concurrent multi-account
  transactions cannot deadlock.
- **`FOR UPDATE` cannot lock a row that does not exist.** An account with no
  `account_balances` row yet has nothing to lock. Not exploitable — with no row, available
  is `0` and any debit is rejected outright — but materialising the row on account creation
  would remove the special case.

## Alternatives considered

- **Compute the balance from `events` at write time.** Perfectly correct and needs no extra
  table, but it is O(history) per write unless it carries its own snapshot mechanism — at
  which point it *is* a write-side balance table, with more moving parts.
- **Keep enforcing against the projection and accept the window.** Rejected. The failure
  mode is silent overdraft under exactly the conditions that matter — bursts of activity on
  one account — and it is not detectable at the point of the decision.
- **Make the projection synchronous again.** This would undo Stage 2 and reintroduce the
  dual-write problem the outbox exists to solve.
- **Reservations only, with no posted/pending split.** Effectively the same design with a
  less explicit vocabulary; keeping the two quantities named separately is what makes the
  Stage 3 saga's hold step a natural extension rather than a new concept.

## Related

- [0003](0003-outbox-and-event-partitioning.md) flagged this tension as unresolved when the
  outbox was introduced. This decision resolves it.
