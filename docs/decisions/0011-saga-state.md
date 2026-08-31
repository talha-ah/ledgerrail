# 0011 — Saga state: record the intent, act, record the outcome

**Status:** Accepted
**Date:** 2026-08-31
**Stage:** 3.2

## Context

Stage 2 solved the dual-write problem with a transactional outbox: the command commits, a relay
publishes at-least-once, and a duplicate dies in the inbox ([0003](0003-outbox-and-event-partitioning.md),
[0005](0005-consumer-inbox.md)). That works because **both halves are systems we own**.

Across a third-party rail the outbox cannot help. At-least-once now means calling a bank twice, and
money moves twice unless the bank itself deduplicates. There is no ordering of "call the bank" and
"record that we called the bank" that avoids both loss and duplication, for the same reason there
was none for Kafka and Postgres — except this time nothing can collapse the two into one commit.

## Decision 1 — Two tables, and the saga log is not the ledger

`payments` holds current state, one row per payment. `payment_steps` is append-only, one row per
*attempt* at a side effect.

Making saga state part of the event journal was tempting in an event-sourced system and is wrong:
retries, timeouts and attempt counts are **operational** facts, not money facts. In the journal they
would be replayed by every rebuild, so every future replay of the ledger would replay our outages.
The journal answers *"where is the money"*; `payments` answers *"where is this workflow"*.

The split itself is the pattern used twice already here — current state beside an immutable history.

## Decision 2 — A state exists to name a crash window

```
initiated → held → quoted → settling → settled → completed
                        ↘        ↘         ↘
                            compensating → compensated / failed
```

A state earns its place only if a process can die *in* it and need recovering differently from its
neighbours. `settling` and `settled` are separate because the gap between them is the one place
where **we genuinely do not know what happened**. `settled → completed` is a different gap: the bank
has paid and only the local capture remains, so recovery needs no third party. Anything that does
not split a crash window like that is a phase, not a state, and belongs nowhere.

Transitions are compare-and-set — `UPDATE … WHERE payment_id = $1 AND state = $2` — so a second
worker still believing an old state updates zero rows and is told, rather than overwriting a state
it never saw. The same discipline as the hold status transition in [0010](0010-holds-as-postings.md).
The transition table is checked twice on purpose: in TypeScript, which catches a programmer
describing a move the machine does not have, and in the `WHERE` clause, which catches a racing
writer. Terminality is *derived* from the table (`TRANSITIONS[state].length === 0`) rather than
declared beside it, so the two cannot disagree.

## Decision 3 — The intent commits before the call, and the state is the claim

`beginStep` writes a row with its idempotency key and **commits before the rail is called**. After
that commit, a crash leaves a row saying *"we were about to do X with key K"* — which is exactly
what a reconciler needs in order to ask the rail what it thinks happened. `finishStep` records the
outcome afterwards. Two transactions per side effect, necessarily, because the call happens between
them: network I/O inside a transaction holds row locks across a round trip we do not control.

That constraint reshapes how work is claimed. `claimPayments` uses `SELECT … FOR UPDATE SKIP LOCKED`
so two orchestrators cooperate rather than queue — but a `FOR UPDATE` claim lives only as long as its
transaction, and the transaction must end before the call. **So the lock is held only long enough to
record the claim; the durable claim is the state plus `next_attempt_at`.** Moving a payment into a
working state pushes its lease forward, and a worker that dies mid-call leaves the payment
reclaimable when the lease expires rather than stranded.

`attempts` is incremented explicitly rather than on every transition, because *"how many times has
this been tried"* and *"how many times has this moved"* stop being the same question as soon as
compensation starts moving it too.

## Decision 4 — The idempotency key identifies a request, not a payment

`payments.request_fingerprint` holds a hash of the caller's parameters, and a key replayed with
different parameters is **refused** rather than silently answered with the original.

Without it, a caller reusing a key with a different amount is told their new payment succeeded when
it was never started — worse than either error it replaces. This is not hypothetical: the test named
*"is idempotent on the caller's key"* had been replaying a **different** request on every run and
passing, because nothing compared the bodies. Adding the fingerprint failed it immediately.

The fingerprint hashes an ordered array joined by NUL rather than `JSON.stringify` of an object:
key order would otherwise leak into the hash, and `JSON.stringify` throws on the `bigint` amount
regardless. The key itself is excluded — it is the question, not the request being asked about.

## Consequences

- **Every step is retryable only because its key is stable.** `stepKey(paymentId, step)` is derived
  from the payment and the step, never per attempt, so a retry presents the bank the same key. A key
  generated per attempt would make each retry a distinct payment to the rail — the double-pay 3.5
  is built to demonstrate.
- **A new attempt can begin while a previous one is unfinished**, which is precisely the ambiguous
  case. It is safe only because the key is unchanged and the rail deduplicates — a bet 3.5 has to
  prove rather than assume. `unfinishedSteps` exists so a reconciler can ask first.
- **The lease is a timeout, so it can be wrong in both directions.** Too short and two workers act on
  one payment; too long and a crashed worker's payment sits idle. There is no value that avoids both,
  which is the usual shape of a failure detector.
- **No orchestrator exists yet.** 3.2 is the machine and its primitives; 3.3 drives it, 3.4 adds
  compensations, and the rails themselves are 3.0.
- **A payment that cannot possibly succeed is refused at the door.** `startPayment` inserts through a
  `SELECT` over `accounts`, so an account that does not exist or is denominated in another currency
  produces no row and no state machine to walk through.

## Related

- [0003](0003-outbox-and-event-partitioning.md) — the dual-write solution that stops working at the
  boundary of a system we do not own.
- [0005](0005-consumer-inbox.md) — where duplicates die when both sides are ours; this decision is
  what replaces it when one side is not.
- [0010](0010-holds-as-postings.md) — the hold this saga takes, and the compare-and-set discipline
  reused here.
