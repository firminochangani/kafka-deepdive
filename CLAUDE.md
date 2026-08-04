# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository purpose

A learning repository: ten progressive Go projects that teach Apache Kafka by building, from the
essentials (`01-`) to architect-level system design (`10-`). The subject is **Kafka** — the
earlier README reference to DynamoDB was a template leftover and has been corrected.

The owner is a backend engineer learning Kafka hands-on from a **software engineer / architect**
perspective, deliberately not an SRE or platform perspective.

## Repository status

Currently `go.mod` (module `kafka-deepdive`, Go 1.26), the root README, and ten project folders
each containing **only a README.md** specifying requirements. **No Go source files exist yet.**
There are no build, lint or test commands.

## Critical constraint: do not write the implementations

The project READMEs are specifications the repository owner intends to implement themselves, as
the entire point of the exercise. Unless the owner explicitly asks for code in a specific project:

- Do **not** create Go source files, `docker-compose.yml` files, migrations, or connector configs.
- Do **not** "helpfully" scaffold a project folder.
- Reviewing, debugging or discussing code the owner has written is welcome and expected.

If asked to help with a project, prefer explaining a concept, reviewing their approach, or
pointing at the relevant Kafka/franz-go documentation over producing the solution.

## Established decisions

These were chosen deliberately; do not silently substitute alternatives.

- **Kafka client:** `franz-go` (`github.com/twmb/franz-go`), including `kadm` for admin work and
  `sr` for Schema Registry. Chosen over `segmentio/kafka-go` (no transaction support) and
  `confluent-kafka-go` (CGo).
- **Infrastructure:** one `docker-compose.yml` per project, KRaft mode only, never ZooKeeper.
  Projects are self-contained; no shared cluster.
- **Domain:** all projects model the same online classifieds marketplace (listings, sellers,
  orders, payments, search, notifications) so project 10 composes into one system.
- **Topic naming:** `<domain>.<entity>.v<n>`, e.g. `marketplace.listings.v1`.
- **`NOTES.md` per project** is the primary learning artifact — experiment results, measured
  numbers, predictions vs reality, and decision records.

## Conventions for new work

When the owner does add code, expect and encourage standard Go module layout: `cmd/<binary>/` for
entrypoints, `internal/` for logic, `migrations/` for SQL. Projects 09 and 10 use
`internal/<component>/` and `services/<service>/` respectively, as specified in their READMEs.

Update this file with real build/lint/test commands once source files exist.
