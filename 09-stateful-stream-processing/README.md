# 09 — Stateful Stream Processing from First Principles

**Level:** Advanced · **Estimated effort:** 4+ evenings — the hardest project here

There is no Kafka Streams for Go. That is an advantage for learning: you are going to build the
parts by hand, which means you will understand what a stream processing framework is doing for
you, and be able to argue about whether you need one.

The goal is not to build a production framework. It is to hit every hard problem in stream
processing — time, state, recovery, repartitioning — and solve each one at least badly.

---

## Concepts covered

- Event time vs processing time vs ingestion time
- Windowing: tumbling, hopping, sliding, session windows
- Watermarks, allowed lateness, and what to do with a record that arrives too late
- Local state stores and the changelog-topic recovery pattern
- Standby replicas and state migration on rebalance
- Stream–table joins and stream–stream (windowed) joins
- Co-partitioning, and repartitioning through a topic to achieve it
- Aggregations, and why emitting on every update is usually wrong
- Suppression / emit-on-close, and the latency-versus-correctness dial
- Reprocessing history, and why event time makes it possible
- Exactly-once in a stateful topology (combining projects 07 and 08)

---

## Scenario

Real-time marketplace analytics and fraud signals, in four topologies of increasing difficulty:

**T1 — Windowed counts.** Views per listing per 1-minute tumbling window, from
`marketplace.activity.v1`. Late data must be handled explicitly, not dropped silently.

**T2 — Stream–table join (enrichment).** Join `marketplace.orders.v1` (stream) against the seller
profile state from project 08 (table) to emit orders enriched with seller data. Handle the case
where the order arrives before the seller profile does — this is the most common real-world
stream-join bug.

**T3 — Stream–stream windowed join.** Join `marketplace.payments.v1` against
`marketplace.orders.v1` within a 30-minute window to detect orders paid but never fulfilled, and
payments with no matching order. Both sides can arrive first; both sides can never arrive.

**T4 — Session windows for fraud signal.** Group a user's activity into sessions with a 30-minute
inactivity gap, and flag sessions matching a suspicious pattern (many views, rapid-fire messaging,
one high-value purchase). Sessions merge when a late record bridges two existing ones — implement
that merge, because it is the part that makes session windows genuinely hard.

---

## Cluster requirements

| Requirement | Value |
| --- | --- |
| Brokers | 3 (KRaft) |
| Schema Registry | yes |
| State store | embedded KV with prefix iteration (Pebble or BadgerDB — you need range scans over window keys) |

### Topics

| Topic | Partitions | RF | Policy | Notes |
| --- | --- | --- | --- | --- |
| `marketplace.activity.v1` | 12 | 3 | `delete` | input |
| `marketplace.orders.v1` | 6 | 3 | `delete` | input |
| `marketplace.payments.v1` | 6 | 3 | `delete` | input, **note the partition count mismatch** |
| `analytics.listing-views-1m.v1` | 12 | 3 | `delete` | T1 output |
| `analytics.orders-enriched.v1` | 6 | 3 | `delete` | T2 output |
| `analytics.payment-anomalies.v1` | 6 | 3 | `delete` | T3 output |
| `analytics.fraud-signals.v1` | 6 | 3 | `delete` | T4 output |
| `<topology>.<store>.changelog.v1` | match source | 3 | **`compact`** | state recovery, one per store |
| `<topology>.repartition.<key>.v1` | as needed | 3 | `delete` | repartition topics |

The partition-count mismatch between orders and payments is deliberate. You must discover that you
cannot join them directly and must repartition one side first.

---

## Functional requirements

1. **Time semantics.** Every record carries an event timestamp in its payload *and* has a broker
   timestamp. Make the time source configurable per topology and demonstrate the difference in
   output. Never let processing time leak in accidentally — that is how a reprocessing run silently
   produces different results from the original.

2. **Windowed state store.** A local store with keys encoding `(logical key, window start)` so you
   can range-scan all windows for a key and all keys in a window. Support: get, put, prefix scan,
   and delete-range for expiring old windows. Bound its size — a windowed store with no expiry is
   a memory leak with a schedule.

3. **Watermark tracking.** Per partition, track the maximum observed event time; the topology's
   watermark is the *minimum* across assigned partitions. Explain in `NOTES.md` why it is the
   minimum, and what a single idle partition does to it. Then implement an idle-partition timeout
   so one quiet partition cannot stall your entire output.

4. **Late data policy.** Three configurable behaviours, all implemented:
   - drop, with a counter
   - accept into an already-emitted window and emit a correction/retraction
   - route to a `late-records` side topic for offline reconciliation

   Run all three and describe which you would choose for a billing aggregate versus a dashboard.

5. **Emit strategy.** Support emit-on-every-update and emit-on-window-close (with a grace period).
   Measure output volume and end-to-end latency for both. This is the correctness/latency dial and
   you should be able to talk about it fluently.

6. **Changelog-backed recovery.** Every state store writes to a compacted changelog topic. On
   startup or after partition assignment, restore state by reading that changelog. Requirements:
   - restore must complete before processing resumes for those partitions
   - measure restore time at 100k and 1M state entries
   - a local checkpoint (offsets + snapshot) to skip full restore on a clean restart

7. **Repartitioning.** For T3, repartition one side through an intermediate topic so both inputs
   are co-partitioned on order ID. Prove co-partitioning holds, and note the latency and cost the
   extra hop adds.

8. **Stream–table join with unresolved lookups.** In T2, when the seller profile is not yet known,
   do not drop the order and do not block. Buffer it with a bounded wait, then either resolve it or
   emit it degraded with a flag. Implement the buffer with an expiry.

9. **Stream–stream join with timeouts.** In T3, you must emit both matches *and* non-matches. That
   means firing an output when a window closes with only one side present — implement the timer
   mechanism that makes "something did not happen" an event.

10. **Session windows with merging.** In T4, when a late record arrives between two existing
    sessions for the same user, merge them into one and retract the two previous outputs.

11. **Exactly-once mode.** Wrap one topology in Kafka transactions (project 07), including the
    changelog writes, so state and output move atomically with the offsets. Then compare against
    the at-least-once version: throughput cost, and what actually goes wrong without it.

---

## Experiments

1. **Event time versus processing time.** Replay a fixed dataset twice: once live, once as a fast
   bulk replay. Under event time, the outputs must be identical. Under processing time, they will
   not be. This experiment is the whole reason event time exists — make sure your output diff is
   byte-identical in the event-time case.

2. **Out-of-order flood.** Inject records with event times up to 10 minutes in the past. Show
   window counts changing after emission, and demonstrate each of your three late-data policies.

3. **Watermark stall.** Stop producing to one partition of a 12-partition topic. Watch all output
   stop. Fix it with idle-partition handling. This symptom — "our aggregates stopped updating but
   nothing is down" — is worth recognising instantly.

4. **State loss and recovery.** Delete the local state directory of a running instance and restart
   it. Confirm it restores fully from the changelog and produces the same results. Measure how
   long the outage lasted.

5. **Rebalance with state.** Run 4 instances, kill one, and measure the time until the orphaned
   partitions' state is restored elsewhere and output resumes. Then implement or simulate a standby
   replica and measure the improvement. Now you know precisely what Kafka Streams' standby
   replicas buy you.

6. **Join miss due to bad partitioning.** Attempt T3 without repartitioning. Show the missed joins
   and explain them in terms of key-to-partition mapping.

7. **Window store growth.** Run T1 for an hour at high cardinality. Chart state store size. Then
   verify your expiry actually reclaims space, and find out whether your KV store returns disk to
   the filesystem or just marks it free.

8. **Session merge.** Construct the exact record sequence that forces two sessions to merge, and
   verify the retraction and the corrected output.

9. **Reprocessing.** Change an aggregation's logic, then reprocess the last 24 hours from scratch
   into a new output topic. Diff old and new. Decide how you would cut consumers over.

10. **Build versus buy.** Having built this, write a page in `NOTES.md` answering honestly: for
    which of these four topologies would you now reach for Flink, Kafka Streams (accepting a JVM
    service), or a database with materialized views instead of hand-written Go? Be specific about
    the threshold. This is the deliverable a staff engineer would actually be asked for.

---

## You are done when

- [ ] You can explain watermarks, and why the topology watermark is a minimum across partitions.
- [ ] You have produced identical output from a live run and a bulk replay under event time.
- [ ] You can explain the changelog-topic recovery pattern and have measured restore time.
- [ ] You can explain co-partitioning and why a join needs it.
- [ ] You have implemented "the thing that did not happen" as an emitted event.
- [ ] You have session-window merging working, including retractions.
- [ ] You can state clearly when you would *not* hand-roll this.

---

## Questions you should be able to answer without notes

- What is a watermark, and what does one idle partition do to it?
- Tumbling, hopping, sliding, session: give a real use case for each.
- What is the trade-off between emit-on-update and emit-on-close?
- Where does a stateful stream processor keep its state, and what happens when the instance dies?
- Why does a stream–stream join require both a window and a state store on both sides?
- How do you detect that something did *not* happen within 30 minutes?
- Your aggregation was wrong for a week. How do you fix history?
- A record arrives two hours late for a billing aggregate that has already been invoiced. What do you do?

---

## Deliverables

```
09-stateful-stream-processing/
├── README.md
├── NOTES.md              ← watermark reasoning, restore measurements, build-vs-buy page
├── docker-compose.yml
├── cmd/
│   ├── t1-windowed-counts/
│   ├── t2-stream-table-join/
│   ├── t3-stream-stream-join/
│   ├── t4-session-fraud/
│   ├── replay/           ← deterministic dataset replay, live and bulk modes
│   └── loadgen/
└── internal/
    ├── statestore/       ← windowed KV over Pebble/Badger, with expiry
    ├── changelog/        ← write-through + restore
    ├── watermark/
    └── timers/           ← the mechanism behind "did not happen"
```

Take the internal packages seriously. They are reusable, and building them is the project.

---

## Reading

- Tyler Akidau's *Streaming 101* and *Streaming 102* — read these **before** you write any code here; they will save you a week
- The Dataflow model paper, if you want the rigorous version
- Kafka Streams docs on windowing, state stores, standby replicas, and suppression — as a reference implementation to compare your design against
- Kafka Streams' `KTable` semantics, specifically the difference between a `KStream` and a `KTable`
