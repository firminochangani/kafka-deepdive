# 05 — Error Handling: Retries, DLQs and Idempotent Consumers

**Level:** Intermediate · **Estimated effort:** 3 evenings

Kafka gives you at-least-once delivery. Everything that makes at-least-once *safe* is your
application's responsibility, and it is almost entirely this project. If you only do one project
in this repository after the basics, do this one.

---

## Concepts covered

- Error taxonomy: retryable vs non-retryable vs poison
- The blocking-retry problem and head-of-line blocking
- Retry topics with tiered backoff, versus in-process retry
- Dead letter queues: shape, metadata, and the redrive tooling nobody writes until it's 3am
- Idempotent consumers and deduplication stores
- Idempotency keys, and why "did I already do this?" is harder than it looks
- Effectively-once as a composition of at-least-once delivery and idempotent effects
- Circuit breaking and pausing consumption when a dependency is down
- Poison-pill isolation and the "skip vs stop" decision
- Ordering guarantees under retry (spoiler: you lose them, and must decide whether you care)

---

## Scenario

**`order-fulfilment`** consumes `marketplace.orders.v1` and, for each order, calls three
downstream systems: a payment capture API (money — must never double-charge), an inventory
service (occasionally returns 409 conflicts), and a notification service (frequently times out
but has already sent the email when it does).

Each downstream fails differently, and each failure demands a different strategy. The whole
point of this project is that "add a retry" is not a strategy.

---

## Cluster requirements

| Requirement | Value |
| --- | --- |
| Brokers | 3 (KRaft) |
| Schema Registry | yes (carry your habits forward from project 04) |
| Postgres | 1 instance — for the deduplication store and the fulfilment state |
| Extras | three fake downstreams with independently injectable failure modes |

Configure the fakes to produce, on demand: transient 503s, permanent 400s, timeouts that
*succeeded* server-side, slow-but-eventually-successful calls, and total outages.

### Topics

| Topic | Partitions | RF | Retention | Purpose |
| --- | --- | --- | --- | --- |
| `marketplace.orders.v1` | 6 | 3 | 7d | main stream |
| `marketplace.orders.retry.5s.v1` | 6 | 3 | 7d | tier 1 |
| `marketplace.orders.retry.1m.v1` | 6 | 3 | 7d | tier 2 |
| `marketplace.orders.retry.15m.v1` | 6 | 3 | 7d | tier 3 |
| `marketplace.orders.dlq.v1` | 6 | 3 | **30d+** | terminal failures |

DLQ retention must be longer than the main topic's. If your DLQ expires before a human looks at
it, you have built a delete-letter queue.

---

## Functional requirements

1. **Error classification.** Every downstream error must be classified before any retry decision:
   retryable, non-retryable, or unknown. Unknown must be treated explicitly (pick a default and
   justify it) rather than falling through to a generic `catch`.

2. **In-process retry with jittered exponential backoff** for short transient failures only,
   with a hard cap on total in-process time. While retrying, the partition must be paused rather
   than blocking the poll loop past `max.poll.interval.ms` — you already learned this the hard
   way in project 03.

3. **Retry-topic ladder.** When in-process retry is exhausted, republish to the next retry tier
   with headers recording: attempt count, first-failure timestamp, last error, original topic,
   original partition, original offset, and the original key. Preserve the key so ordering
   within a tier still follows the partition.

4. **Delayed consumption.** The tier-2 and tier-3 consumers must respect the delay. Implement it
   properly: check the record's timestamp, and if the delay has not elapsed, **pause the
   partition and sleep** rather than spinning or discarding. Explain in `NOTES.md` why a
   per-record `sleep` inside the poll loop is a trap and why a naive "seek back" is worse.

5. **DLQ with everything a human needs.** A record in the DLQ must be sufficient, on its own, to
   understand and reproduce the failure: full original payload, full original headers, the
   complete retry history, the terminal error, and the consumer version that gave up.

6. **DLQ inspection and redrive tooling.** A command that can list DLQ contents with filters,
   show a single record in full, and selectively republish records back to the main topic —
   individually, by filter, or all. Redrive must be idempotent-safe: republishing a record whose
   effect already landed must not double-apply. Also support *discard* with a recorded reason.

7. **Idempotent effects.** For each downstream, make the effect idempotent:
   - **Payments** — a deterministic idempotency key derived from order data, sent to the
     provider, plus a local record of the attempt. Double-charging is the failure you are
     designing against.
   - **Inventory** — conditional update / compare-and-set semantics.
   - **Notifications** — a dedup table keyed on (order ID, notification type), with a decision
     about acceptable duplicate rate.

8. **Deduplication store.** A Postgres-backed processed-message store keyed on something stable.
   Decide and document: is the key the (topic, partition, offset) triple, or a business
   idempotency key from the payload? They behave very differently under redrive, replay and
   partition reassignment — try both and write down which one you would ship, and why the
   offset-based one is a trap for anything you might ever reprocess.

9. **Circuit breaker.** When a downstream's error rate crosses a threshold, stop consuming
   entirely rather than shovelling the whole topic into the DLQ. Resume on a half-open probe.
   Emit an obvious signal while open.

10. **Poison-pill handling.** A record that cannot even be deserialized must not stop the
    consumer forever, and must not be silently dropped. Route it to the DLQ with the raw bytes
    intact.

---

## Experiments

1. **Head-of-line blocking.** With in-process retry only, make one order fail persistently.
   Measure the lag on that partition while it retries. Now do the same with the retry-topic
   ladder and compare. This is the argument for retry topics; own the numbers.

2. **Ordering loss under retry.** Send `order.created` then `order.cancelled` for the same order.
   Make the first one fail into the retry ladder and the second one succeed immediately. Show
   the resulting incorrect state. Then design a fix and implement one of: per-key sequencing
   with a pending-buffer, key-level circuit breaking, or state-machine-guarded application that
   rejects out-of-order transitions. Explain the trade-off you accepted. **This is the most
   important experiment in the project** — it is where most real event-driven systems are quietly
   broken.

3. **The timeout that succeeded.** Configure the notification fake to time out *after* doing the
   work. Retry. Observe the duplicate. Then fix it with the dedup store and prove the fix.

4. **Double-charge drill.** Same idea with the payment fake, then verify with your idempotency
   key that only one capture landed even after five delivery attempts and a manual redrive. Do
   not move on until the ledger balances.

5. **Total outage.** Take a downstream fully offline for 10 minutes under load. Compare two
   behaviours: (a) no circuit breaker — count how many records reach the DLQ; (b) circuit
   breaker — count them again. Then measure how long the backlog takes to drain after recovery.

6. **Redrive at scale.** Get 10,000 records into the DLQ, fix the "bug", and redrive them all.
   Confirm no duplicate side effects and that the ladder does not immediately refill.

7. **Dedup key comparison.** Fill the dedup store using offset-based keys, then replay the topic
   from the beginning (simulating a topic migration or a group reset). Watch every effect
   re-apply despite the dedup store. Repeat with business-key-based dedup and watch it hold.

8. **Dedup store growth.** Project the size of your dedup table at 10k orders/s. Design and
   implement its eviction policy. Then reason about what happens to correctness once you evict —
   what is your effective dedup window, and what happens to a redrive that arrives after it?

9. **DLQ poison loop.** Deliberately create a record that fails in the DLQ-writing path itself.
   Decide what a consumer should do when even the escape hatch fails.

---

## You are done when

- [ ] You can explain why in-process retry and retry topics solve different problems.
- [ ] You have reproduced a double-charge and then made it impossible.
- [ ] You can explain, precisely, what "exactly-once" means for a side effect on an external
      system and why Kafka transactions (project 07) do not give it to you.
- [ ] You can defend your choice of deduplication key against the alternative.
- [ ] Your redrive tool has been used on 10k records without a duplicate effect.
- [ ] You can describe the ordering consequences of a retry topic and how you mitigated them.

---

## Questions you should be able to answer without notes

- Why is a DLQ with only the failed payload and no metadata close to useless?
- A consumer retries a record 5 times in-process. What is happening to the other records in that partition? What about the other partitions?
- Your dedup table is keyed on `(topic, partition, offset)`. Name three operations that silently break it.
- When should a consumer stop consuming rather than route failures to the DLQ?
- How do you make a non-idempotent third-party API call safe to retry when the provider offers no idempotency key?
- Retry topics break per-key ordering. Give two designs that keep it, and their costs.
- What is the difference between "at-least-once with idempotent effects" and "exactly-once"? Is there one, operationally?

---

## Deliverables

```
05-retries-dlq-and-idempotent-consumers/
├── README.md
├── NOTES.md              ← ordering-loss analysis, dedup key decision, outage numbers
├── docker-compose.yml
├── migrations/           ← dedup + fulfilment tables
├── cmd/
│   ├── fulfilment/       ← main consumer
│   ├── retry-tier/       ← delayed consumers (one binary, tier by flag)
│   ├── dlqctl/           ← list / show / redrive / discard
│   ├── downstreams/      ← the three fakes
│   └── loadgen/
└── internal/
```

---

## Reading

- Uber's engineering post on multi-tier retry topics, and Confluent's error-handling patterns write-up
- Stripe's idempotency-key documentation — the clearest public explanation of idempotent APIs
- Anything on the "effectively once" framing; it is the honest description of what you are building here
