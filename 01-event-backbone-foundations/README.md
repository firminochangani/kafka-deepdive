# 01 — Event Backbone Foundations

**Level:** Essential · **Estimated effort:** 1–2 evenings

The first project in the series. Everything later builds on the mental model you form here, so resist the urge to rush it.

---

## Concepts covered

- Broker, topic, partition, offset, log segment
- Record anatomy: key, value, headers, timestamp
- Partitioning and the relationship between key and partition
- Producer basics: synchronous vs asynchronous produce, delivery callbacks
- Consumer groups, group coordinator, partition assignment
- Offset commits: auto-commit vs manual commit, and what "committed" actually means
- Consumer lag as a first-class concept
- The Admin API: creating topics, describing them, inspecting groups

---

## Scenario

You are building the ingestion edge of an online classifieds marketplace. When a seller
publishes, edits, or removes a listing, that fact must be recorded on an event log that
any number of downstream systems can read independently.

Two services:

- **`listing-emitter`** — an HTTP service exposing endpoints to create, update and delete a
  listing. It does not own a database in this project. Its only job is to turn a request into
  an event on the log and return.
- **`listing-indexer`** — a consumer that reads those events and maintains an in-memory
  "search index" (a plain map is fine) that it can dump on demand or log periodically.

---

## Cluster requirements

A single broker running in **KRaft combined mode** (controller + broker in one process). No
ZooKeeper — it is removed from modern Kafka and you should not learn it.

| Requirement | Value |
| --- | --- |
| Brokers | 1 |
| Mode | KRaft, combined (`process.roles=broker,controller`) |
| Listeners | one internal listener for inter-container traffic, one external listener published to the host |
| `offsets.topic.replication.factor` | `1` (the default of 3 will fail on a single broker — find out the hard way) |
| `auto.create.topics.enable` | `false` |
| Extras | a Kafka UI container (AKHQ, kafka-ui, or Redpanda Console) — highly recommended |

Disabling auto-topic-creation is deliberate. Auto-created topics get default partition counts
and hide bugs where you produce to a typo'd topic name.

### Topics

| Topic | Partitions | Replication | Cleanup policy |
| --- | --- | --- | --- |
| `marketplace.listings.v1` | 6 | 1 | `delete` |

Create it explicitly using the Admin API from Go, not the CLI. You should own topic creation
in code at least once.

---

## Functional requirements

1. **Topic bootstrap.** A small command that creates `marketplace.listings.v1` with the shape
   above, is safe to run twice (handle "topic already exists" gracefully), and prints back the
   topic description it read from the broker. ✅

2. **Produce with a meaningful key.** The event key must be the listing ID. The value may be
   JSON for now — schemas arrive in project 04. ✅

3. **Event envelope.** Every event carries, in *headers* (not the payload): event type
   (`listing.created` / `listing.updated` / `listing.deleted`), a schema/version marker, a
   correlation ID, and the producing service name. Keeping metadata in headers so a consumer
   can route without deserializing the body is a habit worth forming now. ✅

4. **Synchronous produce path.** The HTTP handler must not return `201` until the broker has
   acknowledged the record. Return the assigned partition and offset in the response body —
   you will use these to reason about ordering. ✅

5. **Asynchronous produce path.** Add a second endpoint that produces without waiting, with a
   callback that logs the result. Compare the latency of both endpoints under a load generator. ✅

6. **Consumer group.** `listing-indexer` joins group `listing-indexer-v1`, applies events to its
   index, and commits offsets **manually** after applying. Auto-commit is forbidden in this
   project; you need to feel the difference before you are allowed to use the convenience. ✅

7. **Graceful shutdown.** On `SIGTERM` the consumer must stop fetching, finish in-flight
   records, commit, and leave the group cleanly. Note how much faster rebalancing is when a
   member leaves properly versus when you `kill -9` it. ✅

8. **Lag reporting.** A command that, for a given group, prints per-partition: current offset,
   log end offset, and lag. Do not shell out to `kafka-consumer-groups.sh` — read it from the
   Admin API. ⚠️ (TODO)

---

## Experiments

Write your observations down in a `NOTES.md` inside this folder. The notes are the actual
deliverable of this project; the code is just the instrument.

1. **Key → partition.** Produce 1000 events across 50 listing IDs. Confirm every event for a
   given ID lands on the same partition. Now produce with a `nil` key and observe how records
   are distributed instead.

2. **Ordering is per-partition, not per-topic.** Produce `created` → `updated` → `deleted` for
   the same listing, then interleave with other listings. Confirm the ordering guarantee holds
   inside a partition and does *not* hold across the topic.

3. **Partition count vs consumers.** Run 1, then 3, then 6, then 8 instances of the indexer in
   the same group. Record the assignment at each step. Explain what the 8th instance is doing.

4. **Two groups, one topic.** Start a second consumer group on the same topic. Confirm both
   groups receive every event and that their offsets are independent. This is the single most
   important property of Kafka versus a traditional queue — make sure you can articulate why.

5. **Replay.** Reset `listing-indexer-v1` to the beginning and let it rebuild its index from
   scratch. Then reset to a specific timestamp. Then reset to a specific offset on one
   partition only.

6. **The offset lie.** Consume a batch, apply it, crash before committing (`panic` deliberately).
   Restart. Observe the redelivery. Then do the reverse: commit *before* applying, crash, and
   observe the loss. You have now built at-least-once and at-most-once by accident — name which
   is which and be able to say which one production systems almost always pick.

7. **Consumer group states.** Watch a group move through `PreparingRebalance` → `CompletingRebalance`
   → `Stable` while you add and remove members.

---

## You are done when

- [ ] You can explain why the partition count is the upper bound on consumer parallelism.
- [ ] You can state, precisely, what ordering guarantee Kafka gives and what it does not.
- [ ] You can describe what a committed offset is stored *in*, and who stores it.
- [ ] You can reproduce both duplicate delivery and message loss on demand, deliberately.
- [ ] Your lag command's output matches what the Kafka UI shows.
- [ ] `NOTES.md` contains your answers to every experiment above.

---

## Questions you should be able to answer without notes

- Where are consumer offsets stored, and what happens to them if a group is idle for a week?
- Why is increasing a topic's partition count a semi-breaking change for keyed data?
- What does a producer do when it has no key — and did that answer change in recent Kafka versions?
- Why can't you decrease the partition count of a topic?
- What is the difference between "the consumer received the record" and "the offset is committed"?
- If a consumer group has 3 members and one hangs indefinitely without dying, what happens?

---

## Deliverables

```
01-event-backbone-foundations/
├── README.md
├── NOTES.md              ← your experiment write-up
├── docker-compose.yml
├── cmd/
│   ├── topicctl/         ← topic bootstrap + describe
│   ├── emitter/          ← HTTP producer service
│   ├── indexer/          ← consumer
│   └── lag/              ← lag reporter
└── internal/
```

---

## Reading

- Kafka docs: *Design → Persistence*, and the *Producer / Consumer* sections of the API guide
- franz-go: the `kgo` package docs and the `examples/` directory in the repository
- franz-go `kadm` package — the Admin API surface you'll use for topics, groups and lag
