# 0002 — Preventing overdraft under concurrency

**Status:** Accepted
**Date:** 2026-07-26
**Stage:** 1.3 (Layer 2)

## Context

Balances in LedgerRail are a **projection**, not the source of truth — the append-only
`events` journal is. The `balances` table is derived state that `postTransaction`
updates in the same transaction as the event append.

An asset account must never go negative (see [0003] on account types: `system` accounts
*are* allowed to go negative, because that is how money enters a conserved double-entry
system). The obvious implementation is a read-then-check guard inside the command:

```
SELECT amount FROM balances WHERE account_id = $1   -- read
if (current + leg.amount < 0n) throw InsufficientFunds
INSERT/UPDATE balances ...                          -- write
```

This is a **read-modify-write race** (a "lost update"). Two concurrent transfers both
read the pre-state, both independently conclude the debit is affordable, and both commit.

### The reproduction

`src/commands/overdraftRace.test.ts` demonstrates it against a real Postgres:

1. Fund an asset account with 100 (committed, from a `system` account).
2. Fire two concurrent debits of 80 each, on **two separate connections**, each in its
   own explicit transaction, both committing.
3. Both read a balance of 100, both approve, both commit.

Observed result: **balance = −60**. The guard is real code that executes on every
posting, and it protected nothing.

Two prerequisites made the reproduction faithful, both of which are easy to get wrong:

- **Separate connections.** A single pg connection processes statements sequentially, so
  two "concurrent" calls on one client cannot race.
- **Explicit transactions that commit.** A race is about cross-transaction visibility.
  Under autocommit, or without a `COMMIT`, there is nothing to race over.

## Options considered

**1. `SELECT ... FOR UPDATE`** — take a row lock when reading the balance. The second
transaction blocks until the first commits, then re-reads the updated balance and
correctly rejects.
*Pro:* the mechanism is explicit and observable; easy to reason about and to demo.
*Con:* serializes all writers touching the same account — contention on hot accounts.

**2. Atomic conditional update** —
`UPDATE balances SET amount = amount + $delta WHERE account_id = $1 AND amount + $delta >= 0`,
then reject when `rowCount === 0`.
*Pro:* no read-then-write window at all; the database performs check-and-apply in one
statement. Least locking, most elegant.
*Con:* the guard moves into SQL, away from the domain logic; a failed posting is
signalled by a row count rather than a value the command reasoned about.

**3. `SERIALIZABLE` isolation** — let Postgres detect the conflict and abort one
transaction; catch the serialization failure and retry.
*Pro:* strongest correctness guarantee, no explicit locking.
*Con:* requires retry logic everywhere, and aborts increase under load.

## Decision

Use **`SELECT ... FOR UPDATE`** on the balance row of every account being debited.

Chosen because the mechanism is visible in the code and its effect is directly
observable (the second transaction demonstrably blocks), which matters for a project
whose purpose is demonstrating understanding. Option 2 is what would likely ship in a
high-throughput production system, and is recorded here as the known alternative.

## Consequences

- **Lock contention.** All concurrent writers to the same account serialize. Acceptable
  here; would need revisiting for hot merchant accounts at volume.
- **Deadlock risk.** A transaction debiting two accounts can deadlock with one debiting
  them in the opposite order. Mitigation: acquire locks in a deterministic order (sort
  debit legs by `account_id`).
- **`FOR UPDATE` cannot lock a row that does not exist.** An account with no `balances`
  row yet has nothing to lock. Not exploitable here — with no row, `current` is `0` and
  any debit is rejected outright — but the general fixes are to materialize a zero row
  first, or lock the `accounts` row, which always exists.
- **Implicit contract on the caller.** Because the client is injected
  (`postTransaction(input, client)`), the lock is only held for as long as the *caller's*
  transaction. Outside an explicit `BEGIN ... COMMIT`, `FOR UPDATE` acquires and releases
  the lock within its own statement and provides no protection — silently. The command
  must be called inside a transaction; this is documented at the call site.

## Regression coverage

The reproduction test is kept, with its assertion inverted: the same two-connection
harness now asserts the balance settles at **20** and that exactly one of the two debits
is rejected with `Insufficient funds`. The test body is the durable artifact — it fails
if the lock is ever removed. The pre-fix commit remains the reproduction of the bug.
