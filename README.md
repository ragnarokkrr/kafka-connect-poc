# Kafka Connect PoC

Mono-repo for Kafka Connect proof-of-concept experiments. Each PoC is
self-contained under `pocs/<poc-name>/` with its own Docker Compose setup,
connector configs, and documentation.

## Current PoCs

| Directory | Description |
|---|---|
| `pocs/poc1-local-compose/` | CDC and Outbox patterns with Debezium, Kafka 4.0 (KRaft), PostgreSQL |

## Getting started

Navigate to a PoC directory and follow its `README.md`.

## Project structure

See [CLAUDE.md](CLAUDE.md) for repo conventions and PoC isolation model.
