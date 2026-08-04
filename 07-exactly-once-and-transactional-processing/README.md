# 07 — Exactly-Once Semantics and Transactional Processing

**Level:** Advanced · **Estimated effort:** 2–3 evenings

Kafka's transactions are widely misunderstood, usually in the direction of believing they do more
than they do. This project's real goal is for you to be able to say precisely where the guarantee
starts and where it stops.

---

## Concepts covered

- Kafka transactions: transactional ID, producer epoch, transaction coordinator
- The transaction state log, control records, and markers in the partition log
- `read_committed` vs `read_uncommitted`, and the Last Stable Offset (LSO)
- The read-process-write pattern and `sendOffsetsToTransaction` semantics
- Zombie fencing: how epochs kill a partitioned-off old instance
- Transaction timeouts, hanging transactions, and the LSO blocking that follows
- Why EOS is Kafka-to-Kafka only, and what that means for a service with a database
- The performance cost of transactions, measured
- Transactions versus the idempotent-consumer approach from project 05

---

## Scenario

A **`settlement-aggregator`** consumes `marketplace.payments.v1` and produces two derived
streams: `marketplace.settlements.v1` (per-seller running totals) and
`marketplace.settlement-audit.v1` (an audit trail entry per input record). Both outputs, plus the
input offsets, must move as one atomic unit: a consumer of the settlements topic must never see a
total that reflects an audit entry that does not exist, or vice versa.

Then you will extend the same aggregator to *also* write to Postgres, and discover that the
transaction does not stretch that far.

---

## Cluster requirements

| Requirement | Value |
| --- | --- |
| Brokers | 3 (KRaft) |
| `transaction.state.log.replication.factor` | `3` |
| `transaction.state.log.min.isr` | `2` |
| `transaction.max.timeout.ms` | leave at default initially; you will lower it for an experiment |
| Postgres | 1 instance, for the "transaction does not cover this" half |

### Topics

| Topic | Partitions | RF | Notes |
| --- | --- | --- | --- |
| `marketplace.payments.v1` | 6 | 3 | input |
| `marketplace.settlements.v1` | 6 | 3 | output, keyed by seller ID |
| `marketplace.settlement-audit.v1` | 6 | 3 | output, keyed by payment ID |

Note that the two outputs are keyed differently from each other and from the input. Repartitioning
inside a transaction is exactly the realistic case.

---

## Functional requirements

1. **Non-transactional baseline.** Build the aggregator first *without* transactions:
   consume → compute → produce both outputs → commit offsets. Add crash injection between the
   two produces, and between the second produce and the offset commit. Demonstrate:
   - partial output (one topic got the record, the other did not)
   - duplicate output after redelivery
   - divergence between the totals stream and the audit stream

   Build a validator that cross-checks the two output topics against the input and reports
   inconsistencies. This validator is your instrument for the rest of the project.

2. **Transactional read-process-write.** Now do it properly: a transactional producer with a
   stable transactional ID, `read_committed` consumer, and consumed offsets committed *as part of
   the transaction* rather than through the consumer group's own commit path. Requirements:
   - one transaction per processed batch, with a bounded batch size
   - correct abort handling on any processing error
   - the transactional ID must be stable across restarts of the *same logical instance*, and
     distinct between instances — think carefully about how you derive it, and what happens when
     partitions are reassigned

3. **Zombie fencing demonstration.** Start an instance, pause it (`SIGSTOP`) mid-transaction,
   start a replacement that takes over, then resume the original. Show that the zombie is fenced
   when it tries to continue. Record the exact error and explain the epoch mechanic behind it.

4. **Consumer-side visibility.** Run one consumer with `read_committed` and one with
   `read_uncommitted` against the settlements topic while your aggregator aborts transactions on
   purpose. Show the difference. Then find the aborted records' offsets and explain the gaps in
   the offset sequence that a `read_committed` consumer observes — and why offset arithmetic on a
   transactional topic is a trap.

5. **The database boundary.** Extend the aggregator to also write each settlement to Postgres.
   Now demonstrate that a Kafka transaction and a Postgres transaction cannot commit atomically:
   crash between them and show the divergence. Then fix it *without* transactions across systems:
   - store the consumed offset in Postgres in the same transaction as the business write, and
     seek from that stored offset on startup, **or**
   - keep the idempotent-consumer approach from project 05

   Implement one, and describe the other in `NOTES.md` with its trade-offs. Then state the rule
   you will carry to work: what is EOS actually good for, and what should you use instead when a
   database is in the picture.

6. **Hanging transaction.** Lower the transaction timeout, then deliberately stall an instance
   inside an open transaction (pause it, or sleep past the timeout). Observe:
   - the coordinator aborting it
   - what a `read_committed` consumer's lag does while the transaction is open (LSO blocking)

   This is the failure mode that makes on-call engineers hate transactions. Be able to describe
   the symptom — "consumer lag is stuck but production is fine" — and the diagnosis.

7. **Cost measurement.** Run identical workloads with and without transactions, sweeping the
   records-per-transaction from 1 to 10,000. Record throughput, p99 end-to-end latency, and the
   commit overhead. Then find the batch size where transactions become acceptable, and explain
   why one-transaction-per-record is a design error.

---

## Experiments

1. **Prove atomicity.** Under sustained load with random crash injection, kill and restart the
   aggregator 50 times. The validator must report zero inconsistencies between the two output
   topics. Anything else means your transaction boundary is wrong.

2. **Duplicates at the edge.** With EOS working end-to-end inside Kafka, add an HTTP call to an
   external service inside the processing step. Show that the external call still happens twice
   on a retry, despite the transaction. Write down the sentence you would use to correct a
   colleague who says "we use exactly-once so duplicates aren't possible."

3. **Transactional ID collision.** Deliberately give two live instances the same transactional ID
   and observe them fencing each other in a loop. Then explain the correct derivation strategy
   and its interaction with rebalancing.

4. **Rebalance during a transaction.** Trigger a rebalance while transactions are open. Note what
   happens to the in-flight transaction and to the partitions being moved.

5. **Coordinator failure.** Identify the broker hosting the transaction coordinator for your
   transactional ID and kill it under load. Observe recovery, and confirm atomicity survived.

6. **Marker overhead.** Compare on-disk size of a transactional topic against a non-transactional
   one for the same logical data. Account for control records. Then reason about the effect of
   very small transactions on log size and on consumer fetch efficiency.

7. **`read_committed` lag semantics.** Chart consumer lag on a `read_committed` consumer with a
   long-running open transaction. Explain to yourself why lag can be non-zero with an idle
   producer.

---

## You are done when

- [ ] You can state, in one sentence, the exact scope of Kafka's exactly-once guarantee.
- [ ] You can explain the transactional ID's role in fencing, and how you derived yours.
- [ ] You have measured the throughput cost of transactions at several batch sizes on your cluster.
- [ ] You can explain LSO blocking and diagnose "lag is stuck but nothing is broken."
- [ ] You can explain why storing offsets in your own database is sometimes better than EOS.
- [ ] You can decide, for a given service, between EOS and idempotent consumers — and defend it.

---

## Questions you should be able to answer without notes

- What does Kafka's exactly-once actually cover, and what is definitely outside it?
- Why does a transactional producer need a *stable* transactional ID across restarts?
- A `read_committed` consumer reports growing lag; producers are healthy and the topic is quiet. What happened?
- Why does a `read_committed` consumer see non-contiguous offsets?
- What is the correct way to make "consume from Kafka, write to Postgres" not lose or duplicate work?
- What are the two things that must be true for `sendOffsetsToTransaction` to be meaningful?
- When would you deliberately not use transactions despite needing consistency between two output topics?

---

## Deliverables

```
07-exactly-once-and-transactional-processing/
├── README.md
├── NOTES.md              ← the guarantee statement in your own words, cost tables, EOS-vs-idempotence decision
├── docker-compose.yml
├── migrations/
├── cmd/
│   ├── aggregator/       ← modes: naive | transactional | db-offsets
│   ├── validator/        ← cross-topic consistency checker
│   ├── chaos/            ← crash / pause injector
│   └── loadgen/
└── internal/
```

---

## Reading

- KIP-98 — exactly-once delivery and transactional messaging (the design document; read it fully now)
- KIP-447 — scalability improvements to the EOS producer/consumer relationship, which explains why transactional-ID-per-partition advice changed
- Confluent's *Transactions in Apache Kafka* article
- franz-go's transaction documentation and its EOS example
