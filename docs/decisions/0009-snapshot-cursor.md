# 0009 — A snapshot is cursored by the projection, not by the journal

**Status:** Accepted
**Date:** 2026-08-23
**Stage:** 2.7 (found while repairing the demo)

## Context

[Stage 1.6](../plan.html) introduced `balance_snapshots` so that rebuilding the projection is
bounded rather than O(history): the rebuild seeds from the latest snapshot and replays only the
events after its `as_of_event_seq`.

`balancesSnapshot` stamped that cursor with `MAX(global_seq) FROM events` — the head of the journal.
At the time this was indistinguishable from correct, because `postTransaction` wrote `balances`
inside its own transaction: the projection could never be behind the journal, so the journal's head
*was* the projection's watermark.

[0004](0004-write-side-balances.md) moved the projection behind the log in Stage 2.3. The two
numbers diverged, and the snapshot kept recording the wrong one.

## The failure

A snapshot's amounts come from `balances`. Its cursor is a claim about *when those amounts were
true*. Stamping the journal's head asserts the projection had applied every event up to that point,
which after 2.3 is false by default rather than by accident.

A rebuild then seeds from that snapshot and replays only events after the cursor — so **every event
the projection had not yet applied is dropped, permanently, from every future rebuild.**

Observed directly: the demo snapshotted an empty projection at sequence 20, and the subsequent
rebuild produced balances derived from events 21–23 only. The funding at 18 and the transfer at 19
were simply gone.

The invariant did not catch it. `assertInvariant` checks Σ = 0 across all balances, and the
surviving events net to zero on their own — which is the Stage 1.6 finding recurring exactly:
*a global invariant can be satisfied by a subset*.

## Decision — stamp `MAX(last_event_seq) FROM balances`

The projection's own watermark, maintained by `applyToBalances` and already described in 1.2 as the
resume cursor. The amounts come from `balances`, so the cursor must describe `balances` too.

A snapshot taken while the projection lags is then still **valid**: it records true amounts at a
true, lower cursor, and the rebuild replays the extra events to catch up.

### The rejected alternative — refuse to snapshot a lagging projection

Tempting, and wrong. After 2.3 the projection is *always* slightly behind; a guard demanding
equality with the journal's head would refuse almost every legitimate snapshot, and the pressure
would be to weaken or bypass it. The problem was never that the projection lagged — it was that the
snapshot lied about it.

### The guard that is kept

Refuse to snapshot an **empty** projection. Where nothing has been applied there is no honest cursor
to record, and the resulting row would be a claim with no basis. This one cannot be triggered by
normal lag.

## Consequences

- **Snapshot cursors may trail the journal**, and rebuilds replay a few more events as a result.
  That is the correct trade: a slightly larger replay against a snapshot that is true.
- **One global cursor is retained**, not one per account. Per-account cursors were 1.6's original
  bug — the writer stored a per-account `last_event_seq` while the rebuild filtered on the global
  maximum, so every account not at that maximum was silently dropped.
- **Snapshot rows written before this change are suspect** and were deleted from the development
  database. `balance_snapshots` retains history deliberately, so that a bad snapshot can be skipped
  and an earlier one used instead — which means a dishonest row does no damage until the day
  somebody relies on it, and then does a great deal.
- **This class of bug is invisible to the invariant.** Any check that asserts a property of the rows
  that are present says nothing about rows that should have been there. Pair aggregates with a
  cardinality check, or compare against a captured before-state — which is what the demo now does.
- **The demo is the regression test.** It projects, snapshots, writes more events, projects again,
  destroys the projection and rebuilds, then compares amounts against what it captured. Nothing in
  the unit suite covers a snapshot taken from a lagging projection.

## Related

- [0004](0004-write-side-balances.md) — moved the projection behind the log, which is what made the
  journal's head and the projection's watermark two different numbers.
- [0007](0007-event-schema-evolution.md) — the other place a rebuild path was nearly forgotten,
  because nothing in production exercises it.
