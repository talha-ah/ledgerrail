# Flows

`docs/decisions/` records *why*. This records *what it looks like* — one document per
checkpoint, so the shape of the system can be read rather than reconstructed from a diff.

Every document answers the same three questions, in this order:

1. **Before** — the architecture at the end of the previous checkpoint.
2. **The change** — the mechanism this checkpoint introduced, drawn as itself: a sequence, a
   state machine, a race, whichever the mechanism actually is.
3. **After** — the same architecture picture as *Before*, with what moved marked.

The first and third diagrams are deliberately the same picture. **Checkpoint N's _After_ is
checkpoint N+1's _Before_**, unchanged, so the files read end to end as one drawing that grows —
and a piece that quietly stopped being written shows up as a dashed box rather than as nothing.

## Conventions

| Notation | Means |
| --- | --- |
| `[( cylinder )]` | a Postgres table |
| `{{ hexagon }}` | a Kafka topic |
| `[ box ]` | code — a module, a command, a process |
| thick stroke | added by **this** checkpoint |
| dashed stroke | dead, or no longer written, as of this checkpoint |
| 💥 | a crash window: the gap where a failure means something is lost or done twice |

The two class definitions every file uses:

```
classDef new stroke-width:3px
classDef gone stroke-dasharray:5 4
```

## Index

| # | Checkpoint | ADR | Commit |
| --- | --- | --- | --- |
| [1.0](1.0-repo-and-infra.md) | Repo, tooling & local infra | — | `c4915dd` |
| [1.1](1.1-money.md) | The Money type | — | `c4915dd` |
| [1.2](1.2-event-store.md) | Event-store schema | — | `7ed5392` |
| [1.3](1.3-post-transaction.md) | PostTransaction + the double-entry invariant | [0002](../decisions/0002-overdraft-locking.md) | `8e5a1a1` `f0a0d34` `ba83c2e` |
| [1.4](1.4-idempotency.md) | Idempotency keys | — | `a8311d5` |
| [1.5](1.5-balance-projection.md) | Balance projection | — | `b3f39cb` |
| [1.6](1.6-snapshots.md) | Snapshots + replay | — | `add4858` |
| [1.7](1.7-demo-and-drill.md) | Demo script + Stage 1 failure drill | — | `55d1f81` |
| [2.0](2.0-kafka-plumbing.md) | Kafka plumbing | — | `40507b9` |
| [2.1](2.1-outbox.md) | Transactional outbox | [0003](../decisions/0003-outbox-and-event-partitioning.md) | `d4bd17d` |
| [2.2](2.2-relay.md) | The relay | [0003](../decisions/0003-outbox-and-event-partitioning.md) | `3acd864` |
| [2.3](2.3-projection-behind-the-log.md) | Projection behind the log | [0004](../decisions/0004-write-side-balances.md) | `d2d04be` |
| [2.4](2.4-inbox.md) | Inbox & deduplication | [0005](../decisions/0005-consumer-inbox.md) | `f789614` |
| [2.5](2.5-dead-letters.md) | Dead letter queue | [0006](../decisions/0006-dead-letter-queue.md) | `ac3ce92` |
| [2.6](2.6-schema-evolution.md) | Schema evolution | [0007](../decisions/0007-event-schema-evolution.md) | `0ccaf27` |
| [2.7](2.7-stage-2-drills.md) | Stage 2 drills | [0008](../decisions/0008-replication-and-quorum.md) [0009](../decisions/0009-snapshot-cursor.md) | `01e0aa9` |
| [3.1](3.1-holds.md) | Holds | [0010](../decisions/0010-holds-as-postings.md) | `e5b0718` |
| [3.2](3.2-saga-state.md) | Saga state | [0011](../decisions/0011-saga-state.md) | `959033d` |
| [3.3](3.3-orchestrator.md) | The orchestrator | [0012](../decisions/0012-orchestration.md) | `89100f2` |

Not yet written, because not yet built: **3.0** mock rails over HTTP, **3.4** compensations,
**3.5** the ambiguous ack, **3.6** choreography for contrast.

## Adding one

Every completed checkpoint gets a file here, in the same commit as the work. Copy
[`_template.md`](_template.md), and take the *Before* diagram verbatim from the previous
checkpoint's *After* — if it needs editing to be true, that edit is itself the finding, and it
belongs in the write-up.
