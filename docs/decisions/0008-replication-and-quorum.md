# 0008 — Three brokers, RF=3, and giving up writes below a majority

**Status:** Accepted
**Date:** 2026-08-22
**Stage:** 2.7

## Context

Stage 2 ran against a single Redpanda broker with `replicationFactor: 1`. That is enough to
develop against and useless for the question the stage exists to answer: what happens to the
ledger when the log becomes unavailable?

A single broker cannot answer it, because every failure looks the same — the process is gone.
The interesting failures need a cluster: a leader that dies while its replicas live, a survivor
that holds the data but cannot commit, a client that cannot discover who took over.

## Decision 1 — Three brokers, replication factor 3

Three is the smallest cluster that has a majority to lose. Two tolerates nothing (a majority of
2 is 2), and four tolerates exactly one failure just as three does, so an even size buys cost
without resilience.

The topic is created with `replicationFactor: 3`, which in Redpanda means each partition is a
Raft group of three replicas and needs two of them to commit a write.

**Replication factor is a one-way door on a live topic.** `createTopic` skips topics that
already exist, so raising it locally meant deleting and recreating `ledger.events` and losing
its log. In production the equivalent is a partition reassignment, never a delete — worth
knowing which one is being performed and why the destructive version was acceptable here.

## Decision 2 — Below a majority, the system loses write availability, not durability

With two of three brokers stopped, the relay cannot publish. That is the intended behaviour, and
it is chosen rather than tolerated: the alternative — accepting a write that only one replica
holds — trades an outage for the possibility of silently losing an acknowledged event. A ledger
does not make that trade.

`min.insync.replicas=2` was passed at topic creation to express this. **It did not take effect,
and would not have been the mechanism anyway.** Redpanda does not surface it as a topic config
or a cluster property, and the create call accepted it without complaint. The actual guarantee
comes from Raft: an RF=3 partition needs two of its three replicas to commit, whatever any ISR
setting says. On Apache Kafka the same guarantee is assembled differently — `acks=all` plus
`min.insync.replicas=2` plus RF=3 — because there partition data uses leader-and-ISR replication
and Raft governs only the controller quorum.

Recorded because the difference matters when explaining the system, and because the config entry
was silently ignored: **a setting accepted without error is not a setting applied.**

## Decision 3 — The client bootstraps from every broker

`KAFKA_BROKERS` is a list, and `getKafka()` splits it. Seeds are used only for bootstrap — the
client learns the real topology from whichever one answers — but with a single seed, stopping
that broker breaks the client for reasons unrelated to the cluster's health, and a connection
error gets mistaken for a quorum failure. The quorum drill kills brokers 1 and 2 for the same
reason: the observation has to survive the client still having someone to talk to.

## Consequences

- **A broker outage degrades to lag, not to lost or refused writes.** `postTransaction` never
  touches Kafka, so commands keep committing during the outage, the outbox absorbs the backlog,
  and the relay drains it on recovery. This is the transactional outbox
  ([0003](0003-outbox-and-event-partitioning.md)) paying for itself under the failure it was
  designed for, and `drill:quorum` asserts each step: three transactions committed, three outbox
  rows unpublished, zero after recovery.
- **One majority loss produces three distinct client symptoms**, all observed in the drill:
  a produce to a dead leader fails with `ECONNREFUSED`; a partition whose leader died cannot
  elect a replacement (`leaderless_partitions`); and the client cannot refresh its metadata to
  discover any of this, because metadata is itself committed through the controller quorum
  (`Broker 1 not found in the cached metadata`).
- **A surviving leader is not a writable leader.** With two brokers down, only one of three
  partitions went leaderless — the other two kept their leader on the surviving broker and still
  could not commit. That is the sharpest available demonstration that the constraint is a
  majority, not reachability.
- **Failure detection is fast but not instant, and independent of the client's experience.** The
  cluster reported itself healthy for the first ~100ms after losing two of three nodes. The
  producer, meanwhile, failed immediately — it discovers a dead leader by trying to talk to it,
  not by being told. Any check that reads cluster state right after inducing a fault has to wait
  for the control plane to catch up.
- **Three brokers cost memory.** Each runs with `--smp 1 --memory 512M` to stay inside a laptop.
  That is a development-only choice and is not a statement about production sizing.

## Related

- [0003](0003-outbox-and-event-partitioning.md) — the outbox that turns this outage into lag, and
  the partition key whose ordering guarantee depends on the partition count staying fixed.
- [0006](0006-dead-letter-queue.md) — the other half of the availability story: a transient
  failure blocks a partition on purpose, which is what an unreachable broker produces.
