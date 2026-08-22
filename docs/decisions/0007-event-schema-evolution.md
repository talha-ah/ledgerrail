# 0007 — Schema evolution by upcasting at read time

**Status:** Accepted
**Date:** 2026-08-22
**Stage:** 2.6

## Context

`event_version` has been a column on `events` since [1.2](../plan.html) and a hardcoded
`event_version === 1` in the consumer's validation gate ever since. This is the checkpoint
that makes it mean something, driven by a change the system genuinely needs: a multi-currency
ledger eventually has to carry currency **per leg**, not once per transaction.

```
v1: { currency, idempotencyKey, postings: [{ accountId, amount }] }
v2: {           idempotencyKey, postings: [{ accountId, amount, currency }] }
```

The change is deliberately breaking. An additive optional field would have let us skip the
version machinery entirely and prove nothing.

The constraint that makes this tractable: the journal is append-only, enforced by a trigger.
There is no migration available. A v1 event written today will still be a v1 event during a
replay in five years, so whatever reconciles the two shapes runs at **read time, forever**.

## Decision 1 — Upcast, rather than branch or dispatch

Three shapes were considered.

- **Upcast at the boundary** — one pure function converts any version to the newest shape, and
  everything downstream only ever sees that shape.
- **Branch inside the projector** — `switch (event_version)` in `applyToBalances`, one route to
  the balance per version.
- **A handler per version** — a registry keyed by version, each handler complete in itself.

We upcast. `applyToBalances` is where money moves, and the number of code paths through it
should stay at one permanently — adding v3 must not add a second route to the balance. The
branch collapses into a small pure function that needs no pool, no container and no fixtures,
so it can be tested exhaustively in milliseconds.

The rejected options are not wrong in general, and the conditions that would favour them are
worth naming. **Branching** avoids inventing anything and is the right answer when two versions
genuinely *mean* different things rather than merely looking different. **A handler per version**
buys real isolation — but most of a handler here is not version-specific (the inbox claim, the
transaction boundary, the invariant), so it is either duplicated per version or extracted into a
shared core, at which point it is the branch again with indirection on top.

The condition that would reverse this: a version whose events must be *applied* differently, not
just read differently. At that point it is usually a new `event_type` rather than a new version.

### Why the upcast is safe here specifically

`v1ToV2` copies the transaction's currency onto every leg. That is exact rather than a guess,
because the command has always rejected a transaction whose legs referenced accounts of a
different currency — so a v1 event's legs were all in that currency by construction. **An upcast
that has to invent a value is a warning sign**, since the invention becomes load-bearing on
every future replay of history that cannot be corrected.

## Decision 2 — The upcast lives inside `applyToBalances`

`applyToBalances` has three callers: the consumer reading the log, `rebuildBalances` replaying
the journal, and `redrive` replaying a quarantined message. Upcasting at each read boundary
would keep the projector pure, at the cost of three call sites that must each remember.

The rebuild is the one that would be forgotten, because nothing in production exercises it — and
its failure is silent until someone rebuilds months later and gets wrong money. Stage 1.5 already
produced exactly this bug once, when the live path and the rebuild path timestamped balances
differently within a single checkpoint.

So the upcast is the first line of the projector, where no caller can skip it. The test that
holds this in place is a rebuild across a journal containing both versions, asserting the
replayed rows equal the live ones **whole**, not merely in amount.

## Decision 3 — Only `upcast` knows which versions exist

The validation gate checks that `event_version` is *a number*, not that it is a *known* number.
Listing the known versions in both places would be two declarations of one fact, and they would
drift — with the failure mode being a version the gate accepts and the upcaster cannot handle.

Inside the upcaster the knowledge is split across two switches, deliberately, and they cannot
drift because each one type-checks against the other:

- `asVersioned` turns a number into the `VersionPayload` union, or throws. Adding a case here
  fails to compile unless the union has that version.
- `upcast` switches over the union and ends in `assertNever`. Adding a version to the union fails
  to compile unless this switch handles it.

One is the runtime authority, the other is a compile-time check. The `default` branch in `upcast`
is unreachable at runtime by construction; it is an assertion about the type, not a code path, and
should survive any coverage report that complains about it.

## Decision 4 — An unknown version is a dead letter, not a block and not a drop

A consumer that meets v3 is not looking at a corrupt message. It is looking at a message from the
future — a deploy in flight, or a rollback — and the correct statement is *"I am too old to
understand this"*.

Blocking the partition is wrong if the upgrade is an hour away; dropping it is data loss. So
`asVersioned` throws a `PermanentError`, and [0006](0006-dead-letter-queue.md) does the rest:
the message is quarantined, the offset advances, and `dlq:redrive` replays it once the consumer
understands v3. "Permanent" has always meant *will never succeed as this code stands*, and a
deploy is what changes the code.

This composes without either checkpoint knowing about the other, and it holds because the inbox
claim and the projection share one transaction: a rejected event rolls back its own claim, so the
redrive is free to apply it later rather than skipping it as already handled.

## Decision 5 — The wire type does not promise a known version

`EventEnvelope.event_version` is `number`, not `1 | 2`, and so is `Event.event_version`. Typing
either as the union would be the type system making a claim about the outside world that the
outside world has not agreed to — a v99 message can and does arrive, and the `events` column has
no constraint restricting what a future deploy writes.

This was caught in review as a live defect rather than reasoned about in advance: `event_version`
had been left out of the envelope's `Omit` list, so `(1 | 2) & number` resolved back to `1 | 2`
and the wider type never took effect. An intersection combines constraints; it does not replace
them — the same mechanic that produced a `never` in Stage 2.3.

## Consequences

- **Upcasters are permanent.** Replay reads from event zero, so `v1ToV2` ships forever, and a
  future v3 adds `v2ToV3` on top of it rather than replacing it. Old events then pay every hop on
  every replay.
- **This is the real cost of event sourcing, stated plainly.** It does not remove schema-migration
  work; it converts a one-time migration into code maintained indefinitely, in exchange for never
  rewriting history. No option — upcast, branch or handler — allows deleting v1 support while a v1
  event remains in the journal.
- **The command's input is unchanged.** `postTransaction` still takes a single `currency` for the
  transaction, because v2 changes how the event is *written*, not what the ledger permits.
  Cross-currency remains Stage 3's problem, behind a saga. Letting the representation change drag
  the domain change in with it would have made this two checkpoints.
- **The relay needed no changes at all** — it copies `event_version` from the row to the envelope
  and never interprets it. That is the payoff for declaring the envelope explicitly in 2.3 rather
  than deriving it from the table row.
- **Payload interiors are still unvalidated.** The gate checks the envelope; `asVersioned` casts
  the payload to the shape its version implies. Per-version payload validation is the upgrade path
  from here, and is most of what a schema registry sells.

## Related

- [0006](0006-dead-letter-queue.md) — the recovery path for a version this build does not know,
  and the reason an unknown version can be rejected without either blocking or losing it.
- [0005](0005-consumer-inbox.md) — the shared transaction that lets a rejected event leave no
  claim behind, which is what makes a later redrive apply rather than skip.
- [0004](0004-write-side-balances.md) — established that `balances` has exactly one writer, which
  is why the number of code paths *through* that writer is the thing worth minimising.
