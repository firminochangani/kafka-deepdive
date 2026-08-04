# 03 — Consumer Groups, Rebalancing and Scaling Consumption

**Level:** Essential → Intermediate · **Estimated effort:** 2–3 evenings

Most production Kafka pain is consumer-side. This project is where you earn the right to say
"we had a rebalance problem and here's how we fixed it."

---

## Concepts covered

- Group coordinator, group membership, join/sync protocol
- Partition assignment strategies: range, round-robin, sticky, **cooperative-sticky**
- Eager (stop-the-world) vs incremental cooperative rebalancing
- KIP-848: the new broker-side consumer group protocol, and how it changes all of the above
- Static group membership (`group.instance.id`) and why it exists
- `session.timeout.ms`, `heartbeat.interval.ms`, `max.poll.interval.ms` — three timeouts people confuse
- Consumer lag: measuring it, and time-lag vs offset-lag
- Concurrency inside a single consumer while preserving per-key ordering
- Pause/resume, backpressure, and the poll loop contract
- Offset commit strategies and their failure modes
- Rack awareness / fetch-from-follower from a client perspective

---

## Scenario

Marketplace listings must be pushed to a search cluster and a recommendation store. Downstream
calls are slow (50–300ms) and occasionally fail. A naive one-record-at-a-time consumer cannot
keep up with peak traffic, and your first attempt at making it concurrent will silently break
per-listing ordering.

You are building **`projection-worker`**: a horizontally scalable, internally concurrent
consumer that keeps per-key ordering, survives rebalances without duplicating work
unnecessarily, and exposes honest lag metrics.

---

## Cluster requirements

| Requirement | Value |
| --- | --- |
| Brokers | 3 (KRaft) |
| Kafka version | recent enough to support the KIP-848 consumer protocol server-side |
| Extras | a fake downstream HTTP service you control, with injectable latency and error rate |

The injectable downstream is not optional. Half of this project's learning comes from making
the downstream slow and watching what your consumer does about it.

### Topics

| Topic | Partitions | RF |
| --- | --- | --- |
| `marketplace.listings.v1` | 12 | 3 |

Twelve partitions so you can observe non-trivial assignment splits across 5 members.

---

## Functional requirements

1. **Baseline sequential consumer.** One record at a time, process, commit. Measure its ceiling
   in records/s against a downstream with 100ms latency. This number is your control.

2. **Concurrent processing with ordering preserved.** Process records in parallel *without*
   allowing two records with the same key to be in flight simultaneously. Pick an approach and
   justify it in `NOTES.md`:
   - N worker goroutines with records dispatched by `hash(key) % N`
   - one goroutine per partition
   - one in-flight "lane" per key with a bounded lane count

   Whatever you choose, the offset you commit must be safe: never commit an offset whose
   predecessor in the same partition is still in flight. Getting this wrong is the single most
   common data-loss bug in event-driven services — implement the watermark/offset-tracking
   logic deliberately and test it.

3. **Backpressure.** When workers are saturated, the consumer must pause fetching for the
   affected partitions rather than growing an unbounded in-memory queue. Resume when capacity
   frees up. Demonstrate that memory stays flat under sustained overload.

4. **Rebalance-aware lifecycle.** Implement partition-revoked and partition-assigned hooks that
   drain in-flight work for revoked partitions and commit before losing ownership. Prove that
   a scale-up does not reprocess records the old owner had already completed.

5. **Assignment strategy comparison.** Make the strategy configurable and support at least
   `range`, `round-robin`, `cooperative-sticky`, plus the KIP-848 protocol mode.

6. **Static membership mode.** Add a mode that assigns stable `group.instance.id` values, and
   tune `session.timeout.ms` accordingly.

7. **Lag exporter.** Report, per partition: offset lag, and **time lag** (now minus the
   timestamp of the next unconsumed record). Explain in `NOTES.md` why an on-call engineer
   usually wants the second one.

8. **Long-processing guard.** A mode where processing a record deliberately takes longer than
   `max.poll.interval.ms`. Observe the eviction, then fix it properly — twice: once by tuning,
   once by restructuring (pause the partition and heartbeat while working). Explain which fix
   you would defend in a design review.

---

## Experiments

1. **Rebalance cost, eager vs cooperative.** With 12 partitions and 4 members under steady load,
   add a 5th member. Measure end-to-end processing throughput during the rebalance under
   `range` and under `cooperative-sticky`. Record how long throughput sits at or near zero. This
   comparison is the reason cooperative rebalancing exists — get the number yourself.

2. **KIP-848.** Run the same drill on the new consumer group protocol. Note what disappears
   (client-side assignment, the sync barrier) and what changes in the configuration surface.

3. **Rebalance storm.** Set `session.timeout.ms` low and `max.poll.interval.ms` low, then make
   the downstream slow. Produce a self-sustaining storm where members are continuously evicted
   and rejoining. Diagnose it *from the logs and metrics only*, as if you hadn't caused it.
   Write the diagnosis as if it were an incident review.

4. **Rolling restart.** Restart 5 members one at a time, with and without static membership.
   Count the rebalances in each case. This is the concrete answer to "why do we set
   `group.instance.id`."

5. **Ordering violation, on purpose.** Remove your ordering safeguard, run with high concurrency
   and a downstream that randomly delays. Build a checker that detects a listing whose
   `updated` was applied after its `deleted`, and show the corrupted end state. Then re-enable
   the safeguard and show it clean.

6. **Offset commit failure modes.** Compare: auto-commit, commit-after-each-record,
   commit-every-N, commit-on-interval-with-watermark. For each, kill the process at the worst
   possible moment and record what the downstream ends up with — duplicates, loss, or neither.

7. **Consumer that cannot keep up.** Produce at 3× the consumer's capacity for 10 minutes.
   Chart lag growth. Then scale out and chart recovery. Estimate the time-to-drain formula and
   check your estimate against reality.

8. **Fetch tuning.** Sweep `fetch.min.bytes`, `fetch.max.wait.ms` and `max.poll.records`. Show
   the trade-off between per-record latency and consumer efficiency.

9. **Zombie member.** `SIGSTOP` a consumer instead of killing it. Watch the group wait out
   `session.timeout.ms`. Then `SIGCONT` it and observe what the resumed member does when it
   discovers it was fenced.

---

## You are done when

- [ ] You can explain the difference between `session.timeout.ms`, `heartbeat.interval.ms` and
      `max.poll.interval.ms` without hesitating, including which thread each one relates to.
- [ ] You have measured rebalance downtime under two protocols on your own cluster.
- [ ] You can describe a safe offset-commit scheme for concurrent processing, and why the naive
      "commit the highest offset I finished" is wrong.
- [ ] You can diagnose a rebalance storm from symptoms.
- [ ] You can justify a partition count for a given throughput and latency target.
- [ ] `NOTES.md` contains your incident-review write-up from experiment 3.

---

## Questions you should be able to answer without notes

- Your consumers rebalance every 5 minutes in production. List the possible causes in order of likelihood.
- Why does processing records concurrently break at-least-once offset semantics if done naively?
- When is it correct to have more partitions than you currently need? What does over-partitioning cost?
- What is the difference between offset lag and time lag, and when do they disagree dramatically?
- A consumer group shows lag of 2M on one partition and 0 on the others. What are you looking at?
- Why might you prefer one goroutine per partition over a hash-based worker pool?
- What breaks if two application instances accidentally share the same `group.instance.id`?

---

## Deliverables

```
03-consumer-groups-and-scaling/
├── README.md
├── NOTES.md              ← includes the incident review from experiment 3
├── docker-compose.yml
├── cmd/
│   ├── worker/           ← the projection worker, all modes behind flags
│   ├── downstream/       ← fake slow/flaky HTTP dependency
│   ├── loadgen/
│   └── lagexporter/
└── internal/
```

---

## Reading

- KIP-429 — incremental cooperative rebalancing
- KIP-345 — static membership
- KIP-848 — the next generation consumer rebalance protocol
- franz-go docs on consumer groups, and its notes on offset management and rebalance callbacks
