# CLAUDE.md — PoC1: Local Compose

This file provides guidance to Claude Code (claude.ai/code) when working with code in this PoC directory.

All PoC1 logic lives exclusively under `/pocs/poc1-local-compose/`.
Do not place PoC1 files at the repo root or in any other PoC directory.

## Scope and goals

PoC1 validates Kafka Connect as a data pipeline using a **local, manually operated
Docker Compose** setup. There is no CI, no automation, no production hardening.

Target topology:

```
[Postgres (source)] → [Debezium Source] → [Kafka / SMTs] → [JDBC Sink] → [Postgres (sink)]
```

Primary validation goals (in priority order):
1. **CDC** — Change Data Capture via Debezium source connector
2. **Outbox Pattern** — event relay via the Debezium outbox transform
3. **End-to-end routing** — SMTs, topic routing, sink delivery to Postgres

Focus is on **clarity and operability**, not robustness or production readiness.

## Allowed technologies

| Layer              | Technology                              |
|--------------------|-----------------------------------------|
| Orchestration      | Docker Compose                          |
| Broker             | Apache Kafka 4.0 (KRaft)               |
| Source connector    | Debezium PostgreSQL connector           |
| Sink connector      | Debezium JDBC Sink                      |
| Source DB         | PostgreSQL                               |
| Sink DB          | PostgreSQL                               |
| Connect runtime    | `quay.io/debezium/connect:3.2`          |
| Observability      | Kafka UI or equivalent (optional)       |

No Kubernetes, no Terraform, no cloud services.

## Naming conventions

- Directories: lowercase, hyphen-separated (e.g., `connectors/`, `sql/`)
- Docker Compose services: lowercase, hyphen-separated (e.g., `kafka-connect`, `source-db`)
- Connector config files: `<type>-<name>.json` (e.g., `source-debezium-cdc.json`)
- SQL files: `<nn>-<description>.sql` with zero-padded ordering prefix (e.g., `01-init-schema.sql`)
- Topics: dot-separated namespace (e.g., `poc1.source.public.orders`)

## Definition of done

PoC1 is complete when:
- [ ] Docker Compose brings up the full topology with a single `docker compose up`
- [ ] A row inserted into the ingress DB appears in the egress Postgres via Kafka Connect
- [ ] CDC capture is demonstrated and documented
- [ ] Outbox pattern is demonstrated and documented
- [ ] A `README.md` in this directory explains how to run and verify each scenario
- [ ] All configs, scripts, and SQL are committed inside this directory

## Reference documents

- [Research.md](Research.md) — accumulated research findings (append-only log, one section per topic)
- [Connector-Reference.md](Connector-Reference.md) — config properties, Kafka message payloads, and sink behavior for both connectors
- [Manual-Test-Runbook.md](Manual-Test-Runbook.md) — step-by-step manual tests for the CDC pipeline

## Directory structure (planned)

```
pocs/poc1-local-compose/
├── CLAUDE.md          ← this file
├── Research.md              ← research log
├── Connector-Reference.md   ← connector config + payload reference
├── Manual-Test-Runbook.md   ← step-by-step manual tests
├── README.md                ← runbook
├── docker-compose.yml
├── connectors/        ← JSON connector configs
├── sql/               ← init scripts for source/sink DBs
└── scripts/           ← helper shell scripts
```
