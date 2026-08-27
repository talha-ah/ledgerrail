# 0010 — A hold is a posting, not a column

**Status:** Accepted
**Date:** 2026-08-27
**Stage:** 3.1

## Context

A saga reserves a customer's funds before it tries to move them, so that nothing else can spend
the same money while a payment is in flight. `account_balances` has carried an unused `pending`
column since [0004](0004-write-side-balances.md), and the overdraft guard has read
`posted + pending` ever since — so the cheapest possible hold was already provisioned.

## Decision 1 — The hold is an event, and `pending` stays unused

Decrementing `pending` would have taken an afternoon. It would also have been the only number in
this ledger with **no event behind it**: not in the journal, not rebuildable, invisible to every
projection, untouched by Σ = 0. Delete the row and nothing could reconstruct it.

So a hold is what it is in accounting — a balanced posting that moves money out of the customer's
balance into a reserve account it has not yet left the building from:

```
FundsHeld     customer −80,  held_funds +80
HoldCaptured  held_funds −80, destination +80
HoldReleased  held_funds −80, customer +80
```

Working the capture mechanics through is what settled it. With the hold in `posted`, a customer's
available balance falls out of one number and the projector needs no notion of which column a leg
belongs to. Adding `pending` as a second dimension would mean capture has to unwind a reservation
*and* post a payment — two leg groups in one event, and a projector that must know the difference.
One conserved quantity, one code path.

`pending` therefore remains what it always was: the seam for the day *ledger balance* and
*available balance* genuinely need to differ, which a payments orchestrator moving money out does
not yet need.

## Decision 2 — The reserve is a `system` account; the once-only invariant lives on the hold row

`held_funds` could have been an `asset` account, whose non-negative floor would make over-release
impossible at the database level. It is a `system` account instead, because the guard takes
`SELECT … FOR UPDATE` on any non-system account it debits — so **every hold in the system would
serialise on one row per currency.** That is the hot-row reasoning from
[0002](0002-overdraft-locking.md), which exempted the system account for exactly this reason.

The invariant moves to where it actually belongs. A hold is consumed once, enforced by a status
transition under `SELECT … WHERE status = 'held' FOR UPDATE`: a second capture finds no row, and a
concurrent one waits, re-reads and is refused. That is a fact about a hold's lifecycle, not about
an account's balance.

A `CHECK` keeps `status` and `settled_event_id` from disagreeing, so "captured with no settlement
event" is unrepresentable rather than merely unlikely.

## Decision 3 — Distinct event types, and the projector never learns about them

`event_type` had been the single literal `"TransactionPosted"` since 1.2. Holds make it a union,
and the consumer gains a *type* axis alongside the version axis from
[0007](0007-event-schema-evolution.md) — `upcast` now resolves `(type, version)`, with an unknown
type dead-lettering exactly as an unknown version does.

The alternative was to reuse `TransactionPosted` and let the reserve account carry the meaning.
Rejected because the journal must answer *"was this customer paid, refunded, or is their money
still reserved?"* without joining to a side table. That is the question a ledger exists to answer.

**Every payload carries `postings`**, so `applyToBalances` reads `upcast(event).payload.postings`
and never discovers there are four event types. To the read model every event is legs that sum to
zero; the type matters to the write side and to the audit trail, not to the arithmetic. A rebuild
across a journal of transfers, holds and captures asserts this.

## Decision 4 — Idempotency is checked before the guard, not after

Found while writing the hold tests, and **latent in `postTransaction` since 1.4**: the overdraft
guard ran before the idempotency insert, so replaying a key whose original had already spent the
funds failed with `Insufficient funds` instead of returning the original result. The existing
idempotency test could not see it, because both callers race *before* balances move.

Commands now look the key up first and return the original if it is there. Two replay checks
remain, and they are not redundant:

- the `SELECT` at the top handles **retry later** — the caller returns after the original committed;
- the `ON CONFLICT` inside `appendEvent` handles **retry now** — two callers in flight, neither able
  to see the other's uncommitted row.

Remove either and a real scenario breaks. This matters more from 3.5 onward, where a retried
payment after an ambiguous acknowledgement is the entire subject.

## Consequences

- **A customer's ledger balance drops when funds are held**, not when they are captured. For a
  payments orchestrator that is correct — the money is committed the moment it is reserved. A card
  network's "available versus ledger balance" distinction is what `pending` is still there for.
- **Holds expire, and nothing sweeps them yet.** `expires_at` is recorded and capture refuses an
  expired hold; the sweeper that returns the money is 3.4, and until it exists a saga that dies
  mid-flight leaves a customer's funds reserved indefinitely.
- **Four commands share one guard and one append.** `assertPostable` and `appendEvent` were
  extracted because two copies of "may this money move?" had already begun to drift.
- **The reserve is checkable**: `held_funds.posted` must equal the sum of outstanding holds, per
  currency. That is a far stronger assertion than Σ = 0, which any subset of balanced events
  satisfies — it ties a balance to a count of things in a particular state, so over-capture, double
  release, or a leg with the wrong sign all break it.
- **A rebuild replays holds like anything else**, which is the return on choosing an event over a
  column and the reason that choice was worth the extra work.

## Related

- [0002](0002-overdraft-locking.md) — the lock the reserve account is deliberately exempt from.
- [0004](0004-write-side-balances.md) — the write-side balance the guard reads, and the `pending`
  column this decision leaves unused.
- [0007](0007-event-schema-evolution.md) — the version axis this adds a type axis to.
