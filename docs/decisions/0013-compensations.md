# 0013 — Compensations: a forward transaction, with a sweeper behind it

**Status:** Accepted
**Date:** 2026-09-09
**Stage:** 3.4

## Context

[0012](0012-orchestration.md) drove the happy path and left `compensating` parked an hour out with
no handler, which was honest and temporary. Everything that abandons a payment past `initiated`
arrives there, and until now nothing left.

The framing that decides most of what follows: **you cannot un-send money; you send it back.** A
compensation is not a rollback, and nothing about it is an undo.

## Decision 1 — A compensation adds no event type

The ledger movement of a compensation is `HoldReleased`, which has existed since
[0010](0010-holds-as-postings.md). Nothing is deleted and no event is amended — the settle that may
have moved the money stands in the journal beside the reversal that answered it.

That falls out of where the money actually is. `capture` is `settled → completed`, so at every
state that can reach `compensating` the funds are still in `HELD:{ccy}`; `RAIL:{ccy}` receives only
at completion, and `completed` is terminal. A compensation therefore asks the rail to send the money
back and empties the reserve with a posting the ledger already knew how to make.

The consequence is a boundary worth stating: **reversing a `completed` payment is a refund, not a
compensation.** It is a new business transaction in the other direction, and `completed` keeps its
empty row in the transition table.

## Decision 2 — The step log decides whether the rail is owed a reversal

`payments` holds current state, so by the time it reads `compensating` the state it was abandoned
from is gone. Recording that origin would be the obvious fix and the wrong one — `payment_steps`
answers the question directly, and it is append-only:

| settle step | means | reverse? |
| --- | --- | --- |
| no row | never tried | no |
| `failed` | the rail answered *no* | no |
| `succeeded` | the rail has it | yes |
| `started` | we never learned | **yes** |

The last row is the whole rule. "May have" is treated exactly like "is", which is the same
distinction `callRail` enforces when it refuses to stamp a transport error as a learned refusal —
and the reason the predicate is `status !== "failed"` rather than `status === "succeeded"`.

## Decision 3 — The settlement is named by its key, not its reference

`ReverseRequest` carries `settleIdempotencyKey`, derived as `stepKey(paymentId, "settle")`, not the
`railReference` the settle response came back with.

The reference is the version that cannot work. A settle whose answer was lost has no reference, and
that is precisely the settlement a compensation exists to reverse. Same reasoning that made the
settle request a pure function of the payment row in [0012](0012-orchestration.md), applied one step
later: a compensator must be able to name what it is undoing without reading a JSONB blob it may
never have received.

`SettleResponse.status` widens to `"settled" | "reversed"` for the same reason — a rail that can
reverse a settlement has to be able to report one, and `lookup` is where 3.5 will ask.

## Decision 4 — `compensating → failed` is honest, and only because the sweeper is behind it

`abandonTo` returned `null` for `compensating`, which fell through to `reschedule` and retried
forever with a clean record and a budget it never spent. That looked like patience and was a hot
loop.

It returns `failed` now. The money is stranded in the reserve when it does, and that is survivable
for exactly one reason: the hold expires, its payment is terminal, and `sweepExpiredHolds` releases
it. **The sweeper is what makes `failed` an available answer at all** — without it the orchestrator
would be choosing between a hot loop and losing a customer's money.

Abandonment also resets `attempts`. A payment abandoned after five settle failures entered
`compensating` with the budget already spent, so the first transient reverse failure ended it — the
same conflation [0012](0012-orchestration.md) found on the successful moves, one path over.

## Decision 5 — Expiry bounds holding, not returning

`claimHold` takes `requireLive`, true for a capture and false for a release.

Refusing to give a customer's money back because the reservation timed out strands the money the
reservation existed to protect. Worse, it deadlocks: a capture that fails on expiry abandons to
`compensating`, whose only move is a release that would fail for the same reason. Both refusals
became `PermanentError` at the same time — `status` never returns to `held` and time only moves
forward, so a retry cannot help either, and left transient they burned the whole budget first.

Settling a hold also checks for a replay **before** claiming it. That ordering has been wrong since
[0010](0010-holds-as-postings.md) and could not be reached until now, because nothing called release
twice — it is the same bug [0010](0010-holds-as-postings.md) fixed in `postTransaction` and did not
carry across the file. A *different* key settling the same hold still throws, which is the hold's
own status transition refusing a second consumer and is a separate guard.

## Decision 6 — The sweeper's safety is the `NOT EXISTS`, not the TTL

`HOLD_TTL_MS` is a guess about how long a saga takes, and a guess that is wrong once releases money a
payment is still trying to send — possibly mid-settle, with the bank already holding it. So the
sweeper claims only holds no live payment owns:

```sql
AND NOT EXISTS (SELECT 1 FROM payments p
                 WHERE p.hold_id = h.hold_id
                   AND p.state NOT IN ('completed','compensated','failed'))
```

`SKIP LOCKED` is in there for `claimPayments`' reason, but it is an efficiency rather than the
correctness argument: the lock is gone by the time the release runs. What makes a double release
harmless is that the compensator and the sweeper **derive the same key** — `holdKey(holdId,
"release")`, keyed by the hold rather than the payment, because the fact recorded is about the hold
and the sweeper does not always have a payment to name. One transaction per hold, not one for the
batch, so an unreleasable hold cannot roll back the good releases beside it.

Capture keeps its `stepKey(paymentId, "capture")`. A capture has exactly one requester, so nothing
forces the change and churn would buy nothing.

## Consequences

- **The default branch is now unreachable, and untested.** Every non-terminal state has a handler
  and terminal states are never claimed, so `park` can only fire for a state from a future
  migration — which the `CHECK` constraint forbids. It stays as the guard for that day, in the
  position `upcast`'s unknown-version branch occupies, and the test that used to exercise it
  (`"parks a state it cannot drive instead of spinning on it"`) is gone rather than faked.
- **The ambiguous reverse is 3.5's.** A reverse whose own answer is lost leaves a `started` step for
  `unfinishedSteps`, exactly as a settle does. Asking the rail what it believes is the reconciler's
  job, and `lookup` is already declared for it.
- **A tick is global, and that stopped being harmless.** `tick` claims up to ten *due* payments, not
  the one a test is about. A stray `compensating` row used to park quietly; now it compensates,
  releasing a hold through the shared reserve — so `makeClaimable` parks every other payment before
  claiming its own. Every balance assertion in that file was previously depending on which tests had
  run first.
- **A hardcoded account id expired.** `postTransaction.test.ts` asserted `Invalid accounts` against
  `accountId: 100`, meaning *an id no test has reached yet*. 3.4's new files created the hundredth
  account, so the currency check fired first and the test began passing on a branch it was not
  named after. Both ids are derived now. A fixed id is a bet on how much the suite grows.
- **FX still is not posted.** Unchanged from [0012](0012-orchestration.md), and a compensation does
  not make it worse: the reverse moves nothing cross-currency either, because the ledger never
  booked the conversion in the first place.

## Related

- [0010](0010-holds-as-postings.md) — the hold this releases, and the replay ordering it inherited.
- [0011](0011-saga-state.md) — the step log this reads to decide, and the key rule it reuses.
- [0012](0012-orchestration.md) — the orchestrator this adds a case to, and the parked state it fills.
