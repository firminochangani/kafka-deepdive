# 08 — Log Compaction, Event Sourcing and Materialized State

**Level:** Advanced · **Estimated effort:** 3 evenings

This is where Kafka stops being a message bus in your head and starts being a database log you
can rebuild anything from. It is also where retention stops being a config value and becomes an
architectural decision.

---

## Concepts covered

- Retention by time and size vs `cleanup.policy=compact`
- The compaction mechanic: log segments, the cleaner, dirty ratio, `min.cleanable.dirty.ratio`
- Tombstones, `delete.retention.ms`, and how deletes eventually disappear
- `min.compaction.lag.ms`, `max.compaction.lag.ms`, and the head of the log
- Compacted topics as a durable key-value snapshot ("the changelog")
- `compact,delete` combined policy
- Event sourcing vs event-carrying state transfer vs change events
- Materialized views / CQRS read models rebuilt from a log
- Bootstrapping local state on startup, and how long that takes at scale
- GDPR / right-to-erasure on an immutable log — crypto-shredding
- Idempotent, order-tolerant state application

---

## Scenario

Two things to build:

**1. A seller profile store.** `marketplace.seller-profiles.v1` is a compacted topic holding the
current state of every seller, keyed by seller ID. Any service can bootstrap a complete local
copy of all sellers by reading the topic from offset 0 to the end — no API calls, no shared
database. This is the "log as a distributed cache primitive" pattern, and it is one of the most
useful things Kafka does.

**2. An event-sourced listing aggregate.** `marketplace.listing-events.v1` is a *non*-compacted,
long-retention topic of immutable facts about listings (`Created`, `PriceChanged`,
`PhotoAdded`, `Deactivated`, `Reactivated`, `Sold`). The current state of any listing is a fold
over its events. A separate projector maintains a queryable read model, and must be able to
rebuild it from scratch and to run two versions side by side.

The contrast between the two topics is the lesson: one stores *state*, the other stores *facts*,
and they have completely different retention, size and evolution properties.

---

## Cluster requirements

| Requirement | Value |
| --- | --- |
| Brokers | 3 (KRaft) |
| Schema Registry | yes |
| Storage | an embedded KV store for local state (Pebble, bbolt, or BadgerDB) and Postgres for the read model |

### Topics

| Topic | Partitions | RF | `cleanup.policy` | Notes |
| --- | --- | --- | --- | --- |
| `marketplace.seller-profiles.v1` | 6 | 3 | `compact` | current-state snapshot |
| `marketplace.listing-events.v1` | 6 | 3 | `delete`, very long retention | immutable facts |
| `marketplace.listing-readmodel.v1` | 6 | 3 | `compact` | materialized current state |
| `compaction-lab.v1` | 1 | 1 | `compact` | tiny segments, for watching the cleaner work |

For `compaction-lab.v1`, set `segment.bytes` very small (a few KB), `min.cleanable.dirty.ratio`
low, and `min.compaction.lag.ms=0` so compaction happens fast enough to observe interactively.
The defaults will make you think compaction is broken.

---

## Functional requirements

1. **Profile writer.** Publishes full seller-profile snapshots keyed by seller ID. Publishing a
   null value deletes the seller. Every write must be a complete state, not a delta — and you
   should be able to explain why compaction forces this.

2. **Profile store library.** A component that any service can embed to get a read-only, always-
   current view of all sellers:
   - on startup, read from offset 0 to the current end offset (the *bootstrap* phase)
   - signal "ready" only once the end has been reached, and refuse queries until then
   - after bootstrap, keep applying updates continuously (the *tail* phase)
   - handle tombstones by removing the key
   - expose the current high-water mark so callers can reason about staleness

   Bootstrap-before-serve is the part everyone gets wrong. Make the readiness gate explicit and
   demonstrate what a request served during bootstrap would have returned.

3. **Cold-start optimisation.** Persist the local state to an embedded KV store along with the
   offsets it reflects, so a restart resumes from the stored offsets instead of re-reading the
   whole topic. Measure both paths at 1M keys.

4. **Event-sourced aggregate.** Append-only listing events with a per-listing version number.
   Enforce optimistic concurrency: a writer must state the expected current version, and a
   conflicting write must be rejected. Think about where that check can live given that Kafka
   itself will not do it for you — and document the compromise you accept.

5. **Projector.** Folds `marketplace.listing-events.v1` into a Postgres read model, and also
   publishes the resulting current state to `marketplace.listing-readmodel.v1` so other services
   can consume state rather than reimplementing the fold. Requirements:
   - a full rebuild command that drops and reconstructs the read model from offset 0
   - idempotent application, so a rebuild or replay produces identical results
   - version tracking so you can run projector v1 and v2 concurrently against separate tables
     and diff the results — this is how you safely ship a change to projection logic

6. **Time-travel query.** Given a listing ID and a timestamp, reconstruct the state of that
   listing as of that moment by folding only the events up to it. This is the capability that
   sells event sourcing to a product owner; build it once so you know what it actually costs.

7. **Crypto-shredding.** Store seller PII encrypted with a per-seller key held outside Kafka.
   Implement erasure by destroying the key rather than mutating the log. Then articulate why
   tombstoning alone does not satisfy a deletion request on a topic that consumers may have
   already copied.

---

## Experiments

1. **Watch compaction happen.** On `compaction-lab.v1`, write 10 versions of 5 keys. Dump the raw
   log (`kafka-dump-log` or your own full-scan consumer) before and after the cleaner runs.
   Confirm the *head* of the log is never compacted and explain why.

2. **The tombstone lifecycle.** Write a key, tombstone it, and observe: the tombstone is retained
   for `delete.retention.ms`, then removed. Now show the danger — a consumer that was offline
   longer than `delete.retention.ms` bootstraps and never learns the key was deleted, so its
   local state keeps a ghost entry forever. Then design a mitigation and write down which
   retention value you would set in production and why.

3. **Compaction is not deduplication.** Show that a compacted topic can contain multiple values
   for a key at the same time, and that a bootstrapping consumer must therefore apply
   last-write-wins per key rather than assuming one record per key.

4. **Bootstrap time at scale.** Load 1M, then 10M seller profiles. Measure full bootstrap time.
   Then measure it again after compaction has run and after your KV-store checkpoint exists.
   Extrapolate: at what dataset size does bootstrap-on-startup stop being viable, and what would
   you do instead?

5. **Retention that eats your source of truth.** On a *copy* of the listing-events topic, set
   retention to a few minutes. Wait. Now try a full projector rebuild and watch it produce a
   corrupt read model. This is the mistake that turns event sourcing into permanent data loss;
   make it once, here, where it costs nothing.

6. **`compact,delete`.** Configure it on a copy of the profiles topic with a finite retention.
   Determine empirically what happens to a key that has not been updated within the retention
   window. Then decide whether you would ever use this policy for a state topic.

7. **Blue/green projection.** Change your projection logic (e.g. compute a new derived field),
   run v2 alongside v1 into a separate table, diff the outputs, then cut over. Write up the
   procedure as a runbook — this is a genuinely valuable thing to have authored.

8. **Out-of-order state application.** Deliver listing events out of order to the projector (you
   already know how from project 05). Show the corrupt fold. Then make the projector
   order-tolerant using the version number, and show it correct.

9. **Snapshotting the aggregate.** With 100k events on a single listing, measure the cost of
   folding from scratch. Implement periodic snapshots and measure again. Note the new problem you
   just created: snapshot schema evolution.

10. **Size comparison.** For the same logical dataset, compare on-disk size of: the compacted
    state topic, the full event topic, and the Postgres read model. Discuss what you would pay to
    keep, and for how long.

---

## You are done when

- [ ] You can explain compaction's guarantee precisely — including what it does *not* guarantee.
- [ ] You can explain the ghost-key failure caused by `delete.retention.ms`.
- [ ] You have measured bootstrap time at 1M+ keys and know where the ceiling is.
- [ ] You can rebuild a read model from scratch and get a byte-identical result.
- [ ] You can explain when to publish state versus when to publish facts, with concrete criteria.
- [ ] You can answer "how do you delete a user's data from an immutable log" without hand-waving.
- [ ] You have a runbook for shipping a projection-logic change safely.

---

## Questions you should be able to answer without notes

- Does a compacted topic guarantee exactly one record per key? Why does the answer matter?
- Why must records on a compacted topic contain full state rather than deltas?
- What is at the head of a compacted log, and why is it exempt from compaction?
- A service bootstraps 40GB of state from a compacted topic on every deploy. What do you change?
- What breaks if you set a finite retention on your event-sourced topic?
- Event sourcing versus a compacted state topic: when would you choose each?
- How would you satisfy a GDPR deletion request for data that lives on a Kafka topic that eight teams consume?
- Your projection logic had a bug for three weeks. Walk through the fix.

---

## Deliverables

```
08-log-compaction-and-materialized-state/
├── README.md
├── NOTES.md              ← compaction observations, bootstrap measurements, retention decisions
├── RUNBOOK.md            ← the blue/green projection cutover procedure
├── docker-compose.yml
├── migrations/
├── cmd/
│   ├── profile-writer/
│   ├── profile-store-demo/   ← service embedding the bootstrap-and-tail store
│   ├── listing-writer/       ← event-sourced appends with optimistic concurrency
│   ├── projector/            ← versioned, rebuildable
│   ├── timetravel/
│   └── logdump/              ← raw full-scan reader for observing compaction
└── internal/
```

---

## Reading

- Kafka docs: *Design → Log Compaction* (short, and worth reading twice)
- Kafka topic configs: `cleanup.policy`, `min.cleanable.dirty.ratio`, `delete.retention.ms`, `min.compaction.lag.ms`, `segment.bytes`
- Martin Kleppmann, *Making Sense of Stream Processing* — the state/stream duality chapters
- The Kafka Streams documentation on state stores and changelog topics — you are hand-building this, so read what the reference implementation does
