# 02 — Producer Durability, Replication and Throughput

**Level:** Essential · **Estimated effort:** 2–3 evenings

Project 01 gave you a working producer. This one makes you responsible for what happens when
the cluster misbehaves, and for the cost of the guarantees you ask for.

---

## Concepts covered

- Replication, leader/follower, the ISR (in-sync replicas) set
- `acks=0` / `acks=1` / `acks=all` and what each one actually promises
- `min.insync.replicas` and the interaction that makes a topic unwritable
- Leader election, controller failover, unclean leader election and the data it eats
- The idempotent producer, producer IDs, sequence numbers and `max.in.flight.requests.per.connection`
- Retries, `delivery.timeout.ms`, and where duplicates and reordering come from
- Batching: `linger.ms`, batch size, compression, and the latency/throughput curve
- Partition leadership distribution and hot partitions

---

## Scenario

The marketplace now takes payments. You are building **`payment-recorder`**: a service that
writes an immutable record of every payment state transition to Kafka. This log is the source
of truth for reconciliation with the payment provider, so losing an event is a financial
incident and duplicating one is a support ticket.

The same codebase must support a **durability profile** chosen by configuration, so you can run
the identical workload under different guarantees and measure the difference.

---

## Cluster requirements

| Requirement | Value |
| --- | --- |
| Brokers | 3 |
| Mode | KRaft; run the controller quorum as 3 combined nodes or 1 dedicated controller + 3 brokers (your choice — note the trade-off) |
| Listeners | internal + external per broker, distinct host ports |
| `offsets.topic.replication.factor` | `3` |
| `transaction.state.log.replication.factor` | `3` |
| `default.replication.factor` | `3` |
| `min.insync.replicas` (broker default) | `2` |
| `unclean.leader.election.enable` | `false` (you will flip this on purpose later) |

You must be able to stop and start individual brokers without tearing down the cluster.

### Topics

| Topic | Partitions | RF | `min.insync.replicas` |
| --- | --- | --- | --- |
| `marketplace.payments.v1` | 6 | 3 | 2 |
| `bench.throughput.v1` | 12 | 3 | 2 |
| `bench.risky.v1` | 3 | 3 | 1 |

---

## Functional requirements

1. **Durability profiles.** One producer binary, three named profiles selectable by flag or env:
   - `fire-and-forget` — `acks=0`, no retries
   - `leader-ack` — `acks=1`, bounded retries
   - `safe` — `acks=all`, idempotence enabled, effectively unbounded retries within a
     delivery timeout

   Print the fully resolved producer configuration on startup. Knowing exactly what your client
   is configured with — including the defaults you did not set — is a real operational skill.

2. **Payment event stream.** Key by payment ID so all transitions of one payment share a
   partition. Events: `authorized`, `captured`, `refunded`, `failed`. Include a monotonically
   increasing sequence number per payment in the payload so a consumer can *detect* ordering
   violations and duplicates.

3. **An audit consumer.** A consumer that reads `marketplace.payments.v1` and reports, per
   payment ID: gaps in the sequence (loss), repeats (duplicates), and out-of-order arrivals
   (reordering). This detector is the measuring instrument for every experiment below — build
   it before you start breaking things.

4. **Load generator.** Produce a configurable number of events per second for a configurable
   duration, across a configurable number of distinct payment IDs. Report p50/p95/p99 produce
   latency, achieved throughput in records/s and MB/s, and error counts by type.

5. **Cluster inspector.** A command that prints, per partition of a topic: leader, replica set,
   ISR set, and whether the partition is currently under-replicated. Run it continuously in a
   split terminal during failure experiments.

6. **Broker-side error handling.** Handle `NOT_ENOUGH_REPLICAS`, `NOT_LEADER_FOR_PARTITION`
   and `REQUEST_TIMED_OUT` explicitly and distinctly in your logs. Know which of these the
   client retries for you and which it does not.

---

## Experiments

Record every result in `NOTES.md` with numbers, not impressions.

### Durability

1. **Kill the leader mid-produce.** Start a steady load with the `safe` profile. Identify the
   leader of a partition and stop that broker. Watch: produce latency spike, the ISR shrink,
   a new leader get elected, and the client recover. Now confirm with the audit consumer that
   you lost and duplicated nothing.

2. **Repeat with `leader-ack`.** Same drill, `acks=1`. Kill the leader *immediately* after it
   acknowledges but before followers replicate. Get the audit consumer to show you the hole.
   This experiment is the entire argument for `acks=all` — make it reproducible.

3. **Repeat with `fire-and-forget`.** Stop a broker and note that your producer barely notices.
   Quantify the loss.

4. **Make the topic unwritable.** With RF=3 and `min.insync.replicas=2`, stop two brokers.
   Produce with `acks=all`. Explain the exact error and why the partition is readable but not
   writable. This is the failure mode most engineers cannot explain in interviews.

5. **`min.insync.replicas=1` trap.** Run the same load against `bench.risky.v1`. Show that
   `acks=all` on a topic with `min.insync.replicas=1` gives you materially weaker durability
   than you thought you were buying.

6. **Unclean leader election.** Enable it on a test topic, then engineer a situation where an
   out-of-sync replica becomes leader. Measure the truncation with the audit consumer. Turn it
   back off and write down why it exists at all.

### Ordering and duplicates

7. **Reordering without idempotence.** Disable idempotence, set `max.in.flight.requests.per.connection`
   to 5, force retries (kill a leader or use a network pause on a broker container). Get the
   audit consumer to report out-of-order records for a single key.

8. **Now enable idempotence** and reproduce the exact same conditions. Explain, in terms of
   producer ID and sequence numbers, why the reordering disappears.

9. **Duplicates the idempotent producer cannot prevent.** Have your application crash after
   sending but before recording that it sent, then restart and resend. Note that producer
   idempotence is scoped to a producer session and does not survive this. Write down what
   *would* fix it — you will build that in projects 06 and 07.

### Throughput

10. **The linger curve.** Fix the load, then sweep `linger.ms` across 0, 5, 20, 100 and record
    throughput and p99 latency at each point. Plot it if you like. Find the knee.

11. **Compression.** Sweep `none`, `gzip`, `snappy`, `lz4`, `zstd` with a realistic payload.
    Record throughput, produced bytes on disk, and producer CPU. Then explain where
    decompression cost lands and why compression is a *batch* property.

12. **Hot partition.** Send 80% of your load to a single key. Show the effect on that partition's
    lag and on the consumer instance that owns it. List three ways to fix it and the cost of each.

13. **Batch size vs record size.** Find the record size at which your throughput becomes
    network-bound rather than request-bound.

---

## You are done when

- [ ] You can draw the ISR mechanic on a whiteboard and explain what makes a replica fall out.
- [ ] You can explain why `acks=all` alone is not a durability guarantee without `min.insync.replicas`.
- [ ] You have personally caused, observed and then prevented reordering on a single key.
- [ ] You can quote your own measured numbers for the latency cost of `acks=all` on this cluster.
- [ ] You can explain what the idempotent producer does and does not protect against.
- [ ] `NOTES.md` has a table of every sweep with real numbers.

---

## Questions you should be able to answer without notes

- What does the leader do with a produce request when `acks=all` and one follower is slow?
- A partition has RF=3, ISR=[1]. `min.insync.replicas=2`. What can readers do? What about writers?
- Why does enabling idempotence historically constrain `max.in.flight.requests.per.connection`, and what is the current limit?
- What is the difference between `retries`, `request.timeout.ms` and `delivery.timeout.ms`, and which one should you actually tune?
- Your producer's p99 latency is 400ms and throughput is fine. Name four possible causes.
- When would you deliberately choose `acks=1` in a real system?

---

## Deliverables

```
02-producer-durability-and-replication/
├── README.md
├── NOTES.md
├── docker-compose.yml
├── cmd/
│   ├── recorder/         ← producer with durability profiles
│   ├── auditor/          ← loss / duplicate / reorder detector
│   ├── loadgen/
│   └── clusterinfo/      ← leader / replicas / ISR inspector
└── internal/
```

---

## Reading

- Kafka docs: *Replication*, *Configuration → Topic-Level Configs*
- KIP-98 (idempotent producer and transactions) — read the idempotence half now, the rest in project 07
- KIP-101 and KIP-279 on leader epoch and truncation, if you want to understand *why* unclean leader election loses data
