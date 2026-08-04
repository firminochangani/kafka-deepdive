# 04 — Schema Contracts and Evolution

**Level:** Intermediate · **Estimated effort:** 2–3 evenings

A topic is a public API with an indefinite retention period and consumers you do not control.
This project is about the discipline that keeps that from becoming a liability. It is the most
architect-flavoured project in the essential half of the series.

---

## Concepts covered

- Schema Registry: subjects, versions, schema IDs, the wire format
- Avro and Protobuf as Kafka payload formats, and how they differ in evolution semantics
- Compatibility modes: `BACKWARD`, `FORWARD`, `FULL`, `*_TRANSITIVE`, `NONE`
- Which change is safe in which direction, and the upgrade *order* it implies
- Subject naming strategies: topic-name, record-name, topic-record-name
- Multiple event types on one topic, and the trade-off versus one topic per type
- Schema references, and evolving a shared type
- Tombstones and schema-less nulls
- Contract testing and CI enforcement of compatibility
- Defaults, optionality, enums, and the fields that quietly break everything

---

## Scenario

The marketplace's listing events are now consumed by four independent teams. You cannot
coordinate a simultaneous deploy across all of them, ever. Your job is to evolve the
`Listing` contract seven times without breaking a single consumer — and to prove it.

You will also build the ordering events as **Protobuf** while listings stay **Avro**, so you
experience both and can argue for one.

---

## Cluster requirements

| Requirement | Value |
| --- | --- |
| Brokers | 3 (KRaft) |
| Schema Registry | 1 instance (Confluent Schema Registry, or Apicurio / Redpanda's registry) |
| Extras | a UI that renders the registry (optional but pleasant) |

### Topics

| Topic | Partitions | RF | Format |
| --- | --- | --- | --- |
| `marketplace.listings.v1` | 6 | 3 | Avro + Schema Registry |
| `marketplace.orders.v1` | 6 | 3 | Protobuf + Schema Registry |
| `marketplace.activity.v1` | 6 | 3 | Avro, **multiple event types on one topic** |

Set the registry's global compatibility to `BACKWARD`, then override it per subject where an
experiment requires it.

---

## Functional requirements

1. **Schema-aware producer and consumer for listings (Avro).** Use the standard wire format
   (magic byte + 4-byte schema ID + payload). Understand what those five leading bytes are — do
   not treat the serializer as a black box. Be able to hexdump a record and read the schema ID
   out of it by eye.

2. **Schema-aware producer and consumer for orders (Protobuf).** Same registry, different
   serialization strategy. Note the differences in how field addition/removal and enums behave.

3. **Registry client tooling.** A command that can: register a schema, list subjects, list
   versions of a subject, fetch a schema by ID, set a compatibility mode, and — most
   importantly — **run a compatibility check without registering**. That last one is what
   belongs in CI.

4. **A consumer that survives.** `listing-indexer` from earlier projects, now schema-aware, must
   keep running untouched across every one of the seven evolutions below. If you have to restart
   or redeploy it because of a schema change, the change was not backward compatible and you
   must explain why.

5. **Multi-type topic.** `marketplace.activity.v1` carries `ListingViewed`, `ListingFavourited`
   and `SearchPerformed` with a record-name or topic-record-name subject strategy. The consumer
   must dispatch on the actual schema, not on a header string.

6. **Compatibility gate.** A check that fails with a non-zero exit code if a schema in your
   repository is incompatible with what is registered. This is the artifact you would actually
   put in a pipeline; treat it as the deliverable, not an afterthought.

7. **Tombstones.** Produce a null-valued record with a valid key on the listings topic and make
   your consumer handle it. Note what the deserializer does with a null value and why that
   matters for project 08.

---

## The seven evolutions

Apply these in order to the `Listing` schema. For each, before you try it, **predict** whether it
is backward compatible, forward compatible, both, or neither. Then find out. Log prediction vs
reality in `NOTES.md` — the ones you get wrong are the valuable ones.

1. Add an optional field `condition` with a default.
2. Add a required field `currency` with no default.
3. Remove an optional field that had a default.
4. Remove a field that had no default.
5. Rename `title` to `headline`.
6. Change `price` from `int` to `double`. Then try `double` → `int`.
7. Add a new symbol to an existing enum `Category`. Then remove one.

Then, for the two or three changes that were *not* safe, design and execute a migration that
achieves the same end state without breaking consumers. Expect to need one of: an aliased field,
a dual-write period with both fields populated, or a new topic version. Write down which
technique you used and the deploy order it required (producers first, or consumers first — and
why the answer is different for `BACKWARD` versus `FORWARD`).

---

## Experiments

1. **Deploy-order proof.** Under `BACKWARD` compatibility, deploy a new producer while the old
   consumer keeps running, unchanged. Show it works. Now switch the subject to `FORWARD` and
   demonstrate the case where the *consumer* must be deployed first. Be able to state the rule
   in one sentence per mode.

2. **The unknown-schema-ID failure.** Point a consumer at a registry that does not have the
   schema ID it just read (simulate by clearing/pointing to a fresh registry). Observe the
   failure mode. Now consider: what does this do to your recovery story during a registry
   outage, and what would you cache?

3. **Registry down.** Stop the registry with producers and consumers running. Determine
   empirically: can they continue? For how long? What is cached and what is not? Then answer the
   design question: is the Schema Registry in your critical path, and should it be?

4. **Payload size comparison.** Produce identical logical data as JSON, Avro-with-registry, and
   Protobuf. Compare uncompressed and compressed on-disk size. Combine with your compression
   findings from project 02 and state the real saving — plain JSON versus Avro after `zstd` is
   a different story than before it.

5. **`NONE` compatibility, one week later.** Set a subject to `NONE`, make three incompatible
   changes, then try to replay the topic from offset 0 with a single consumer build. This is what
   "we'll fix the schema discipline later" actually costs. Write the postmortem.

6. **Schema references.** Extract a shared `Money` type used by both `Listing` and `Order`.
   Evolve `Money`. Observe the blast radius across both subjects, and decide whether you would
   share types across teams in a real organisation.

---

## You are done when

- [ ] You can state what `BACKWARD` compatibility permits and which side must deploy first.
- [ ] You correctly predicted at least five of the seven evolutions before testing.
- [ ] You have a working CI-style compatibility gate.
- [ ] You can explain the five bytes at the start of a schema-registry-serialized record.
- [ ] You can argue Avro vs Protobuf vs JSON for a specific scenario, with your own size numbers.
- [ ] You can explain when to put multiple event types on one topic and when not to.

---

## Questions you should be able to answer without notes

- Compatibility is `BACKWARD`. A team wants to add a mandatory field. What do you tell them?
- Why does the ability to replay a topic from the beginning constrain your schema policy far more than steady-state consumption does?
- What is the difference between `BACKWARD` and `BACKWARD_TRANSITIVE`, and when has the difference actually bitten someone?
- Why can renaming a field be safe in Avro but not in a naive JSON consumer?
- Should the Schema Registry be a hard dependency of your producer's startup path? Defend it.
- A single topic carries three event types. How does a consumer that cares about one of them avoid deserializing the others?

---

## Deliverables

```
04-schema-contracts-and-evolution/
├── README.md
├── NOTES.md              ← predictions vs reality table, migration write-ups, postmortem
├── docker-compose.yml
├── schemas/
│   ├── listing/          ← every version, numbered, kept forever
│   ├── order/
│   └── activity/
├── cmd/
│   ├── registryctl/      ← register, check, list, set-compat
│   ├── compat-gate/      ← CI check
│   ├── emitter/
│   └── indexer/
└── internal/
```

Keep every schema version in the repository. Being able to see the evolution history in one
directory is part of the lesson.

---

## Reading

- Confluent docs: *Schema Registry → Schema Evolution and Compatibility*
- Avro specification: *Schema Resolution* — read this section properly, it is short and it is the whole game
- Protobuf docs on updating message types
- The `franz-go/pkg/sr` package for the registry client, and its serde helpers
