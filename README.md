# kafka-deepdive

A progressive series of small-to-medium Go projects for learning Apache Kafka by building, from
the essentials through to advanced stream processing and system design.

The perspective throughout is that of a **backend engineer and software architect**, not an SRE:
the focus is on what an application owner must understand, decide and defend — delivery
guarantees, schema contracts, ordering, idempotence, state, topic design — rather than on broker
tuning, cluster capacity planning or Kafka operations.

## Goal

By the end of project 10 you should be able to design an event-driven system for an unfamiliar
domain on a whiteboard, justify every topic, key, partition count and retention setting you
propose, and tell first-hand stories about failures you have caused, diagnosed and fixed.

## Ground rules

- **Language:** Go. **Client:** [`franz-go`](https://github.com/twmb/franz-go) — pure Go and the
  most feature-complete option, including transactions and the modern consumer protocol.
- **Infrastructure:** each project is self-contained with its own `docker-compose.yml` and its own
  KRaft cluster (no ZooKeeper). Nothing is shared between projects.
- **Write your notes.** Every project asks for a `NOTES.md`. That file — not the code — is the
  real output. The point of the experiments is to have measured and broken things yourself; if you
  do not write down what you observed, you will not be able to recall it in a design discussion
  six months later.
- **Predict before you run.** Several projects ask you to guess an outcome before testing it. The
  predictions you get wrong are where the learning is.

## The projects

| # | Project | Level | Focus |
| --- | --- | --- | --- |
| [01](01-event-backbone-foundations/) | Event Backbone Foundations | Essential | topics, partitions, offsets, keys, consumer groups, replay |
| [02](02-producer-durability-and-replication/) | Producer Durability and Replication | Essential | acks, ISR, `min.insync.replicas`, idempotent producer, throughput tuning |
| [03](03-consumer-groups-and-scaling/) | Consumer Groups and Scaling | Essential → Intermediate | rebalancing, cooperative-sticky, KIP-848, safe concurrency, lag |
| [04](04-schema-contracts-and-evolution/) | Schema Contracts and Evolution | Intermediate | Schema Registry, Avro/Protobuf, compatibility modes, CI gates |
| [05](05-retries-dlq-and-idempotent-consumers/) | Retries, DLQs and Idempotent Consumers | Intermediate | retry topics, DLQ + redrive, deduplication, circuit breaking |
| [06](06-transactional-outbox-and-cdc/) | Transactional Outbox and CDC | Advanced | dual-write problem, outbox pattern, Debezium, Kafka Connect |
| [07](07-exactly-once-and-transactional-processing/) | Exactly-Once and Transactional Processing | Advanced | Kafka transactions, fencing, `read_committed`, the DB boundary |
| [08](08-log-compaction-and-materialized-state/) | Log Compaction and Materialized State | Advanced | compaction, tombstones, event sourcing, CQRS read models |
| [09](09-stateful-stream-processing/) | Stateful Stream Processing | Advanced | event time, watermarks, windows, joins, changelog recovery |
| [10](10-event-driven-architecture-capstone/) | Event-Driven Architecture Capstone | Architect | topic design, sagas, replay, migration, multi-region, game days |

Do them in order. Each one assumes the mechanisms and the tooling from the ones before it, and
several ask you to reuse a component you built earlier (project 02's auditor and project 05's
dedup store show up repeatedly).

## Recurring domain

All ten projects model the same system — an online classifieds marketplace: listings, sellers,
orders, payments, search, notifications. This is intentional, so that the capstone assembles into
a coherent whole rather than ten unrelated exercises.

## What this series deliberately does not cover

Broker capacity planning, JVM and OS tuning, cluster upgrades, rack/AZ topology, ACL and
authentication administration, and quota configuration. You will meet these as *constraints* —
what to ask a platform team for, and why — but not as things you configure yourself.

## How this repository was generated

The project specifications were generated with Claude Code (Opus 5) from the prompt below.

> Read the README to understand the purpose of this project. I am a software engineer focused in
> backend engineering, and I intend to learning Kafka by doing small projects, that being said:
>
> Generate as many projects as needed to help me understand on practice the fundamentals and the
> advanced topics of Kafka. From a software engineer's/software architect's perspective not an SRE
> or DevOps Engineer;
>
> Each project should be placed at a subfolder, with a README describing the requirements. Just the
> README, the implementation is to be done by me;
>
> Do not generate any code;
>
> Enumarate the projects "01-project-name" from essential to advanced;
>
> By the time I finish completing these projects, I want to feel confident enough to talk by
> first-hand practical experience how a system could leverage Kafka.
>
> Before you generate anything, is there anything I might have missed from my request?

That final question produced four follow-up decisions, which shaped everything above:

| Decision | Choice | Why it mattered |
| --- | --- | --- |
| Go client | `franz-go` | Transactions, cooperative rebalancing and KIP-848 are unavailable in some clients; projects 07 and 09 would have been impossible with `segmentio/kafka-go` |
| Ecosystem scope | Schema Registry + Avro/Protobuf, Kafka Connect + Debezium CDC, and stateful stream processing | Added projects 04, 06 and 09 |
| Local infrastructure | Per-project `docker-compose.yml`, KRaft | Each project declares its own cluster shape — 1 broker where that suffices, 3 where replication is the point |
| Granularity | ~8–10 larger projects rather than 15+ small exercises | Each project bundles several related concepts into a realistic service |

One addition was made beyond the original request: every project carries an **Experiments**
section of deliberate failure drills — kill the leader mid-produce, trigger a rebalance storm,
feed a poison message, break a schema, let a replication slot fill the disk. Reading about ISR
shrink teaches nothing; watching a producer block because `min.insync.replicas` cannot be
satisfied is what becomes first-hand experience.
