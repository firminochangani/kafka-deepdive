# 10 — Capstone: Designing and Operating an Event-Driven System

**Level:** Advanced / Architect · **Estimated effort:** open-ended; treat it as a small product

Projects 01–09 taught you mechanisms. This one is about judgement: topic design, evolution
strategy, boundaries between services, replay, migration, multi-region, and cost. It is
deliberately less prescriptive — you are the architect now, and the main artifact is a set of
documents you could defend in a design review.

---

## Concepts covered

- Topic design: granularity, event types, naming, versioning, ownership
- Event notification vs event-carrying state transfer vs event sourcing — choosing per boundary
- Choreography vs orchestration; the saga pattern and compensating actions
- Partition-count sizing and the cost of getting it wrong in each direction
- Consumer contracts, SLOs, and lag-based alerting from an application owner's perspective
- Replay and backfill as first-class product capabilities
- Topic migration: moving to a `v2` topic with live consumers
- Multi-tenancy, quotas, and noisy-neighbour containment
- Cross-cluster replication (MirrorMaker 2) for DR and regional locality — from a design angle
- Tiered storage and long-retention economics
- Testing strategy: unit, contract, integration with Testcontainers, and end-to-end
- Cost modelling: what a topic actually costs per year

---

## Scenario

Bring the marketplace together as a coherent system with at least five services communicating
only through Kafka:

| Service | Owns | Responsibility |
| --- | --- | --- |
| `catalog` | listings | listing lifecycle, publishes listing events (outbox, project 06) |
| `orders` | orders | order lifecycle, saga coordinator |
| `payments` | payments, ledger | authorise / capture / refund, idempotent (project 05) |
| `search` | its index | materialized read model from listing events (project 08) |
| `notifications` | delivery log | reacts to events across the system, deduplicated |

Plus `analytics` reusing a topology from project 09, and no shared database anywhere. If two
services need the same data, one of them owns it and publishes it.

---

## Part 1 — Design documents (do this before writing code)

These are the deliverable. Write them properly.

1. **`docs/topic-catalog.md`** — every topic: name, owner, key, partitions and the reasoning for
   that number, replication factor, retention or compaction policy, schema subject, expected
   throughput and record size, consumers, and the compatibility mode. Include a naming convention
   and justify it. Include estimated annual storage cost per topic.

2. **`docs/event-taxonomy.md`** — for each service boundary, state whether you chose notification,
   state transfer, or event sourcing, and why. Be explicit about the coupling each choice creates:
   fat events reduce chatter and increase schema coupling; thin events do the opposite.

3. **`docs/order-saga.md`** — the order flow as a saga across `orders`, `payments` and `catalog`:
   happy path, every failure branch, every compensating action, and the timeout for each step.
   Then argue choreography vs orchestration for *this* flow and commit to one.

4. **`docs/slos.md`** — per consumer group: acceptable end-to-end latency, acceptable lag, what
   alerts fire at what threshold, and what the on-call action is. Include what happens when a
   consumer is down for an hour, and for a week — and check that your retention supports the
   answer.

5. **`docs/adr/`** — at least five architecture decision records, one per significant choice. Reuse
   the ones you wrote in projects 06 and 07.

---

## Part 2 — Build it

1. **Five services, Kafka-only integration.** No service reads another's database. No synchronous
   HTTP between them except where you explicitly justify it in an ADR.

2. **The order saga.** Implemented with real compensations: a failed payment capture must release
   inventory and mark the listing available again. Every step must be idempotent and every timeout
   must have a defined behaviour.

3. **Replay as a product capability.** A tool that can replay a topic or a range into a *target of
   choice* — the live consumer group, a shadow group, or a file — with filters by key, time range
   and event type. Rate-limited, so a replay cannot take out the live system. This is the tool you
   will wish you had during an incident.

4. **Backfill.** Rebuild the `search` index from scratch while it continues serving live traffic
   from the current index, then atomically cut over. Zero downtime, and you must be able to prove
   the two indexes agree.

5. **Topic migration to v2.** Take one topic through a real breaking change:
   - stand up `...v2` with the new incompatible schema
   - dual-publish, or bridge v1 → v2 with a translator consumer
   - migrate consumers one at a time
   - verify equivalence, then retire v1

   Document the runbook. Include the rollback plan, and be honest about the window in which
   rollback stops being possible.

6. **Multi-tenancy.** Add a second tenant with very different traffic characteristics. Decide:
   topic per tenant, tenant in the key, or a shared topic with a tenant header. Implement your
   choice and demonstrate that one tenant's traffic spike does not starve the other. Then explain
   what you would need from the platform team (quotas) to actually enforce it.

7. **Testing pyramid.** Concretely:
   - unit tests for pure event-handling logic, with no Kafka involved
   - contract tests that assert your schemas remain compatible (project 04's gate, wired into CI)
   - integration tests using Testcontainers to spin up a real broker per test suite
   - one end-to-end test that drives the full order saga and asserts the terminal state

   Make the whole suite runnable with one command and fast enough that you actually run it.

8. **Observability from an application owner's view.** Per service: consumer lag, processing
   latency histogram, error rate by classification, DLQ depth, and end-to-end event age. Wire it
   to Prometheus + Grafana and build one dashboard you would put on a wall. Explicitly *not* broker
   internals — that is the platform team's dashboard, and knowing the difference is part of the
   skill.

9. **Cross-cluster replication.** Stand up a second Kafka cluster and replicate a subset of topics
   with MirrorMaker 2. Explore: topic renaming, offset translation for consumer groups, and what a
   failover actually requires of your consumers. You are not becoming an SRE here — you are
   learning what active/passive costs the *application*, and why consumer group offsets are the
   hard part of any DR story.

---

## Part 3 — Chaos and game days

Run each of these as a game day: predict the impact first, then run it, then compare.

1. Kill one broker under full load. Then two.
2. Take the Schema Registry down for 15 minutes.
3. Take Postgres down for one service.
4. Make one consumer group 2 hours behind, then let it catch up. Does anything downstream break
   because of the burst?
5. Deploy a bad consumer that DLQs everything, notice it, roll back, and redrive.
6. Fill a partition with a poison message that crashes every consumer instance in a loop.
7. Introduce a schema change that passes the compatibility gate but is semantically wrong (a field
   whose *meaning* changed). Note that no tool caught it, and write down what process would have.
8. Simulate a full region loss with MirrorMaker 2 and fail consumers over to the second cluster.

Write a short incident report for each: detection, diagnosis, mitigation, and what you would change.

---

## Part 4 — The interview test

The real acceptance criteria for this repository. Without notes, in front of a whiteboard, you
should be able to:

1. Design an event-driven system for an unfamiliar domain, and justify every topic, key, partition
   count and retention setting you propose.
2. Answer "why Kafka and not SQS/RabbitMQ/Pub-Sub" with specific properties, not vibes — and name
   the cases where you would pick the other thing.
3. Explain the delivery-guarantee spectrum and where you would sit for a payment flow, an audit
   log, and a click stream.
4. Describe three ways a system like this loses data, and how each is prevented.
5. Describe how you would evolve a schema consumed by four teams you cannot coordinate with.
6. Explain how you would replay six months of history without taking production down.
7. Explain what you would monitor and what you would page on.
8. Tell a story about something you broke, how you diagnosed it, and what you changed. You should
   have eight or nine of these from `NOTES.md` by now.

If any of these still feels shaky, the fix is almost always to go back and redo a specific
experiment from projects 02, 03, 05 or 09 — not to read more.

---

## Deliverables

```
10-event-driven-architecture-capstone/
├── README.md
├── docs/
│   ├── topic-catalog.md
│   ├── event-taxonomy.md
│   ├── order-saga.md
│   ├── slos.md
│   ├── runbooks/
│   │   ├── topic-migration-v1-to-v2.md
│   │   ├── search-index-backfill.md
│   │   └── consumer-lag-response.md
│   ├── adr/
│   └── game-days/        ← one incident report per chaos experiment
├── docker-compose.yml    ← the full stack, plus the second cluster
├── services/
│   ├── catalog/
│   ├── orders/
│   ├── payments/
│   ├── search/
│   └── notifications/
├── tools/
│   ├── replayctl/
│   └── migratectl/
├── observability/        ← Prometheus + Grafana config, dashboard JSON
└── test/
    └── e2e/
```

---

## Reading

- Martin Kleppmann, *Designing Data-Intensive Applications* — chapters 11 and 12 especially
- *Designing Event-Driven Systems*, Ben Stopford — free from Confluent, and the closest thing to a
  textbook for this project
- Sam Newman on event-driven collaboration and its failure modes
- MirrorMaker 2 documentation (KIP-382) — particularly the offset translation section
- Kafka tiered storage (KIP-405) for the long-retention economics conversation
