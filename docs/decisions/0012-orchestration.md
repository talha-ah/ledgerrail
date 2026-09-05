# 0012 — The orchestrator: one step per tick, and only two steps are sagas

**Status:** Accepted
**Date:** 2026-09-04
**Stage:** 3.3

## Context

[0011](0011-saga-state.md) built the state machine and its primitives and drove nothing. The
question 3.3 answers is what actually calls them, and in what order — and the first thing that
falls out is that most of the machinery is not needed for most of the payment.

## Decision 1 — Saga machinery only where atomicity is unavailable

Four moves make a payment. Two of them touch a system we do not own:

| move                          | side effect                  | shape                   |
| ----------------------------- | ---------------------------- | ----------------------- |
| `initiated → held`            | `holdFunds` — our database   | one transaction         |
| `held → quoted`               | FX call                      | intent / call / outcome |
| `quoted → settling → settled` | bank call                    | intent / call / outcome |
| `settled → completed`         | `captureHold` — our database | one transaction         |

The begin/act/finish dance is the expensive answer to _"you cannot atomically call a bank and record
that you called it."_ Where both halves are ours, atomicity is still available, and using the saga
shape anyway would be ceremony that buys nothing and **opens a crash window that did not have to
exist**. The hold and the capture are one transaction each and write no step row.

`quote` gets a step row but **no state**, which is 0011's own rule applied honestly: a state must
name a crash window that recovers differently from its neighbours. Crashing mid-quote leaves the
payment in `held` with an unfinished quote step, and the recovery is _ask again_, because a quote
moves no money. That is why there is no `quoting`.

## Decision 2 — A tick advances one step, and `settling` is not a step boundary

There is no "run this saga" function. There is only _advance this payment by one step_, and the loop
claims whatever is due.

This is what makes **the resumer and the driver the same code**. A payment claimed in `settling` is
not a recovery path — it is the same `switch`, on that state, doing what it always does. Nothing in
the orchestrator distinguishes a fresh payment from one a dead worker left behind, so there is no
second code path to keep correct.

`settling` is the middle of the settle step rather than a resting place, so one tick carries a
payment through it. The states a tick _ends_ on are the ones where stopping is safe.

## Decision 3 — The settle request is a pure function of the payment row

`quote_id` moved onto `payments` for this. Given the row, the request is fully determined:
key, quote, destination, amount, both currencies. Two consequences, both load-bearing:

- A retry sends a **byte-identical** request, which is what the rail's deduplication is keyed on.
- A reconciler that finds a payment stuck in `settling` reconstructs what was sent **without
  reading a JSONB blob**. Same reasoning that makes `stepKey` derived rather than generated.

## Decision 4 — An expired quote goes back to `held`, and only from `quoted`

The rail refuses an expired quote, so a stale `quoted` payment would otherwise deadlock: settle
fails, backoff, quote is staler, forever. `quoted → held` is a **re-pricing, not a rollback** — the
funds are still reserved, only the price went stale.

It is checked before `settling` is entered, with a margin, because a quote that expires _during_ the
call produces a refusal that arrives after `settling` is committed — and from there the only way out
is forward. `settling` deliberately has no edge back to `held`: once the bank may have the money,
re-pricing is not a thing that can happen.

The consequence is real and correct: **a settlement that never landed and whose quote has since
expired cannot be retried.** It compensates, and the customer re-initiates at a new price.

## Decision 5 — The default branch is load-bearing

`compensating` is not terminal, so `claimPayments` offers it every tick until 3.4 exists. An
orchestrator that claims a state it has no handler for is a hot loop against its own database.
Unhandled states are parked an hour out with the reason recorded, which also makes 3.4 a pure
addition rather than an edit.

Abandonment is state-dependent for the same reason the transition table has no `held → failed` edge:
from `initiated` nothing has moved and `failed` is honest; from anywhere else a hold exists, and
skipping compensation strands a customer's money.

## Decision 6 — A fake is only useful where it is strict

The rail is a port with an in-process fake; 3.0 substitutes an HTTP adapter without the orchestrator
learning which side it is on. A fake's failure mode is being **more permissive than the thing it
stands in for**, so the fake refuses an unknown quote, an expired one, a settlement that does not
match its quote, a target amount truncated to zero, and a key reused for a different request. Every
check it skipped would be a test that proved nothing — which is exactly how 3.2's idempotency test
passed for weeks while replaying a different request every run.

It also raises `PermanentError` where a retry cannot help. That is the contract 3.0's adapter inherits: it maps status codes onto the same distinction, and the orchestrator sees only this one.

The mapping has to happen at the edge. `parseRate` throws a plain `Error`, so a malformed rate reaching the orchestrator would be retried as transient — the fake validates its whole rate table at construction instead, and 3.0's adapter has the harder version of the same duty for rates that arrive off the wire.

The distinction that mapping encodes is narrower than "will a retry help": **permanent means the rail
answered _no_; transient means we do not know.** A transport error belongs in the second class even
when it looks hopeless, because the request may have reached the bank and only the answer was lost —
which is why `callRail` records a step `failed` only on a permanent error and otherwise leaves it
`started`, where `unfinishedSteps` will hand it to 3.5's reconciler.

One place the fake is deliberately *permissive*, and it is load-bearing: the re-price path assumes a
rail will **issue a new quote under a key it has already answered**, once the old one has expired. A
strictly key-idempotent rail would return the same dead quote forever and Decision 4 would never get
a new price. Quotes move no money, so keying them at all is our audit convenience rather than a rail
contract — but 3.0's adapter has to honour it, or the margin logic has nowhere to go.

## Consequences

- **The happy path is proven; no timeout is.** A fake that always answers instantly cannot exercise
  a slow call, a lost response or a partition. That is Stage 4's subject, and 3.5's ambiguous ack is
  staged by hand until then.
- **`attempts` is a per-step retry budget, not a per-payment move counter.** It is incremented only
  by the failure path and reset by each completed step — so `MAX_ATTEMPTS` means _five failures of
  the thing currently being tried_. Bumping it on successful moves reached the first settle with the
  budget three-fifths spent and abandoned healthy payments after two rail attempts, which is exactly
  the conflation [0011](0011-saga-state.md) named and then failed to avoid.
- **There are two budgets, because there are two shapes of loop.** `attempts` bounds retries _within_
  a step and is reset by each completed step. It therefore cannot bound a cycle that _completes_ a
  step each time round — `quoted → held → quoted` re-prices successfully every lap, resetting the
  budget it would need to spend, and runs forever with `attempts` reading zero. What bounds that is
  the count of `quote` rows in `payment_steps`: append-only, never reset, which is precisely what an
  append-only log is for. Two counters looked like duplication until one of them could not do the
  other's job.
- **The lease is fenced on a token, not on its own deadline.** `next_attempt_at` answers _when_ a
  payment may be retried; `lease_token` answers _who holds it_. One column cannot do both — two
  workers can share a deadline, and separately, `TIMESTAMPTZ` keeps microseconds while a JS `Date`
  keeps milliseconds, so the equality fence that conflated them matched nothing and the orchestrator
  claimed no work at all.
- **The state at failure is not the state at entry.** The settle handler commits `settling` before
  the call, so the failure handler re-reads the row rather than fencing on what it was claimed with.
- **FX is priced but not posted.** The ledger moves source currency into `RAIL:{ccy}`; the rate and
  target amount are workflow facts. Cross-currency postings need an FX position account, a
  per-currency Σ = 0, and a rounding-residual policy — and `assertPostable` currently sums Σ across
  all legs regardless of currency, which is fine only while nothing posts two.

## Related

- [0010](0010-holds-as-postings.md) — the hold this orchestrator takes and captures.
- [0011](0011-saga-state.md) — the machine this drives, and the crash-safety rule it obeys.
