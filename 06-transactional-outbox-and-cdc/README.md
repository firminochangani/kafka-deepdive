# 06 — The Dual-Write Problem: Transactional Outbox and CDC

**Level:** Advanced · **Estimated effort:** 3 evenings

Every service that owns a database and publishes events has this problem, whether or not anyone
on the team has noticed. This project makes you break it deliberately, then fix it two different
ways, then choose.

---

## Concepts covered

- The dual-write problem and why "write to DB, then publish to Kafka" is unsound
- The transactional outbox pattern, and its polling and CDC-driven variants
- Change data capture: the Postgres WAL, logical replication, replication slots
- Debezium: connectors, snapshots, streaming, LSNs, slot lag
- Kafka Connect: workers, tasks, converters, single message transforms, distributed mode
- The outbox event router, and event topic routing from a table
- Log-based CDC vs application-level events — the "database schema as public API" problem
- Snapshot-then-stream, initial loads, and reprocessing
- Tombstones from deletes, and `ExtractNewRecordState`
- Connect's own failure modes: dead connectors, offset topics, slot bloat

---

## Scenario

The `orders` service owns a Postgres database. It must publish `order.created` /
`order.status_changed` events. Currently it commits the transaction and then produces to Kafka —
and you are going to prove that this loses and invents events.

You will then implement **three** approaches and compare them:

- **A** — naive dual write (the baseline you are trying to kill)
- **B** — outbox table + application-level poller/publisher
- **C** — outbox table + Debezium CDC reading the Postgres WAL

And separately, you will run **direct CDC** on the `orders` table itself (no outbox) to feel why
that is a different, more dangerous design decision.

---

## Cluster requirements

| Requirement | Value |
| --- | --- |
| Brokers | 3 (KRaft) |
| Schema Registry | yes |
| Postgres | 1 instance, `wal_level=logical`, sufficient `max_replication_slots` / `max_wal_senders` |
| Kafka Connect | 1–2 workers in **distributed** mode with the Debezium Postgres connector installed |
| Extras | registry + Connect UI, and a consumer that detects loss/duplication (reuse project 02's auditor) |

Distributed mode with two workers, not standalone. You want to see task rebalancing when a
worker dies.

### Topics

| Topic | Notes |
| --- | --- |
| `marketplace.orders.v1` | the public event stream, all three approaches write here |
| `cdc.orders.public.orders` | raw Debezium output for the direct-CDC comparison |
| `cdc.orders.public.outbox` | raw Debezium output for the outbox table |
| Connect internals | config, offset and status topics — create them with RF=3 and correct cleanup policies |

---

## Functional requirements

1. **Approach A: the naive dual write.** Commit the DB transaction, then produce. Add a crash
   injection point *between* the two. Also add a mode where the produce fails after the commit
   succeeds. Prove both failure directions with the auditor:
   - committed but never published (an event the world never learns about)
   - published but rolled back (an event for an order that does not exist)

   Write down which one is worse and why the answer depends on the consumer.

2. **Approach B: outbox with a poller.** An `outbox` table written inside the same transaction as
   the business change. A publisher process that reads unpublished rows in order, produces them,
   and marks them published. Requirements:
   - the business write and the outbox write share one transaction, always
   - ordering per aggregate is preserved
   - the publisher is safe to run as multiple instances, or explicitly single-instance with a
     lock — decide which, and implement the locking if you go that way
   - crash between produce and mark-published must produce a duplicate, never a loss — and you
     must be able to state that this is why consumers still need idempotence (project 05)
   - published rows are cleaned up on a retention policy

3. **Approach C: outbox with Debezium.** Same outbox table, but no poller: Debezium tails the WAL
   and Connect routes rows to Kafka. Requirements:
   - use the outbox event router pattern so `aggregate_type` selects the destination topic and
     `aggregate_id` becomes the record key
   - the payload on the topic must be the business event, not a Debezium envelope wrapping a row
   - schema-registry-serialized output, consistent with project 04

4. **Direct CDC on the business table.** Configure a separate connector that captures the
   `orders` table itself. Compare its output to your curated events. Then answer, in `NOTES.md`:
   what happens to every downstream consumer when a backend engineer renames a column? Under
   which circumstances would you still choose direct CDC?

5. **Snapshot and initial load.** Load 100k historical orders, then start the connector cold and
   let it snapshot. Observe the snapshot-to-streaming transition. Then trigger an incremental /
   ad-hoc re-snapshot of a subset without losing your place in the stream.

6. **Delete semantics.** Delete a row. Trace what arrives on the topic, including the tombstone.
   Configure the transform chain so your consumer sees a usable delete event rather than a raw
   `before`/`after` envelope.

7. **Connector lifecycle tooling.** A command that lists connectors, shows status and task state,
   pauses/resumes, restarts a failed task, and prints the current WAL/LSN position and slot lag.

---

## Experiments

1. **Break the naive version reproducibly.** Run 10k orders with crash injection at 1% and get
   the auditor to report exact counts of phantom and missing events. Keep this number; it is the
   most persuasive artifact in this project.

2. **Outbox poller latency.** Measure end-to-end latency (commit → available to consumer) as you
   sweep the poll interval and batch size. Note the floor you cannot get below, and compare it
   to Debezium's latency for the same workload.

3. **Poller throughput ceiling.** Find the order rate at which the poller cannot keep up. Watch
   the outbox table grow. Then reason about which knobs help and which just move the problem.

4. **Two pollers, one outbox.** Run two publisher instances without a lock. Produce the
   duplicate-and-reorder failure. Then fix it (advisory lock, `SKIP LOCKED` partitioning by
   aggregate hash, or leader election) and verify per-aggregate ordering holds.

5. **Kill Postgres mid-stream.** With Debezium running under load, restart Postgres. Confirm no
   loss and identify what guarantees the replication slot gave you.

6. **Kill a Connect worker.** With two workers and several connectors, kill one. Watch task
   rebalancing. Measure the gap in the event stream.

7. **The replication slot trap.** Stop the Connect worker (or pause the connector) and keep
   writing to Postgres for a while. Watch WAL accumulate because the slot is not advancing.
   Understand that an abandoned slot will eventually fill the disk and take the database down.
   Then write down the monitoring you would put on it. This is the single most common
   production incident caused by CDC.

8. **Schema change under CDC.** Add a column, drop a column, rename a column, and change a
   type — on the business table with direct CDC running. Record what reaches consumers each
   time. Then do the same to the *outbox* table and note how much less happens. That contrast
   is the architectural argument for the outbox.

9. **Replay from CDC topics.** Delete your downstream store entirely and rebuild it from the CDC
   topic. Determine whether the topic alone is sufficient, or whether you need a fresh snapshot,
   and what that implies about retention.

10. **Ordering across aggregates.** Confirm that Debezium gives you per-table WAL order but that
    your topic-level ordering depends on partitioning by key. Show a case where two consumers
    disagree about the order of events touching two different orders, and decide whether that
    matters.

---

## You are done when

- [ ] You can explain the dual-write problem to a skeptical engineer in under a minute, with your own failure counts.
- [ ] You have implemented the outbox pattern twice, with measured latency for each.
- [ ] You can explain what a Postgres replication slot is and how it can take down a database.
- [ ] You can argue outbox vs direct CDC as a coupling decision, not a technology preference.
- [ ] You understand why the outbox still requires idempotent consumers, and can say exactly why.
- [ ] You have rebuilt a downstream store from CDC topics from scratch.

---

## Questions you should be able to answer without notes

- Why can't you just use a distributed transaction between Postgres and Kafka?
- Outbox with a poller versus outbox with CDC: what does each cost, operationally?
- Direct CDC publishes your table schema to every consumer. Name three ways that hurts, and one situation where you would accept it.
- What exactly does Debezium store to know where it left off, and where does it store it?
- A Debezium connector has been paused for two days. What is the state of your database?
- Does the outbox pattern give you exactly-once delivery? If not, what does it give you?
- Snapshotting a 500GB table would take hours. What are your options?

---

## Deliverables

```
06-transactional-outbox-and-cdc/
├── README.md
├── NOTES.md              ← failure counts, latency comparison, outbox-vs-CDC decision record
├── docker-compose.yml
├── migrations/           ← orders + outbox tables
├── connectors/           ← connector configuration (JSON)
├── cmd/
│   ├── orders-api/       ← modes: naive | outbox
│   ├── outbox-publisher/ ← approach B
│   ├── connectctl/       ← connector + slot lifecycle tooling
│   ├── auditor/
│   └── loadgen/
└── internal/
```

Write the outbox-vs-CDC decision in `NOTES.md` as a proper ADR — context, options, decision,
consequences. This is the format you will use to defend the choice at work.

---

## Reading

- Debezium documentation: the Postgres connector, and the *Outbox Event Router* transform
- Postgres docs on logical decoding and replication slots
- Kafka Connect: *Concepts* and *Distributed Mode*
- Anything by Gunnar Morling or Martin Kleppmann on turning the database inside out
