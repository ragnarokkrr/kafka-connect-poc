# PoC1 Research Log

Accumulated research findings for PoC1. Each section is a self-contained investigation.

## Rules

- One section per research topic, ordered chronologically.
- Each section must include: date, objective, findings, sources.
- Do not delete or rewrite previous sections — append only.
- Keep findings factual; opinions and decisions go in `CLAUDE.md`.

---

## 1. Debezium Connect Container Image

**Date:** 2026-01-30
**Objective:** Determine which Docker image to use for Kafka Connect with Debezium plugins, and which versions are compatible with Kafka brokers 3.8, 3.9, and 4.0.1.

### Image hierarchy

```
quay.io/debezium/kafka:<version>          # bare Kafka binaries, JDK, kafka user
  └── quay.io/debezium/connect-base:<version>   # Connect runtime, entrypoint, plugin tooling
        └── quay.io/debezium/connect:<version>   # all Debezium connector JARs pre-installed
```

### What `debezium/connect` includes out of the box

- All Debezium source connectors (Postgres, MySQL, MongoDB, SQL Server, Oracle, DB2, Spanner, Vitess, Informix, IBM i)
- Debezium JDBC sink/source connector (`debezium-connector-jdbc`)
- REST extension and scripting support (optional, via env vars)
- Apicurio Schema Registry converters (optional, via `ENABLE_APICURIO_CONVERTERS=true`)

### Key paths inside the container

| Path | Purpose |
|---|---|
| `/kafka/connect/` | `KAFKA_CONNECT_PLUGINS_DIR` — auto-discovered plugin subdirectories |
| `/kafka/external_libs/` | `EXTERNAL_LIBS_DIR` — optional shared libs (Apicurio, OTel, scripting) |
| `/kafka/libs/` | `MAVEN_DEP_DESTINATION` — global classpath JARs (Jolokia, etc.) |

### Exposed ports

| Port | Purpose |
|---|---|
| 8083 | Kafka Connect REST API |
| 8778 | Jolokia (JMX over HTTP) |

### Entrypoint behavior

The `docker-entrypoint.sh` script:
1. Resolves Kafka bootstrap servers from env vars or Docker links.
2. Converts `CONNECT_*` env vars into `connect-distributed.properties` keys.
3. Conditionally loads optional libs (`ENABLE_DEBEZIUM_SCRIPTING`, `ENABLE_APICURIO_CONVERTERS`, `ENABLE_OTEL`).
4. Configures JMX, Jolokia, optional Prometheus exporter, Java Flight Recorder.
5. Validates required settings (`BOOTSTRAP_SERVERS`, `GROUP_ID`, storage topics).
6. Starts `connect-distributed`.

### Adding extra connectors

Drop JARs into a subdirectory of `/kafka/connect/`. The entrypoint auto-discovers them. Alternatively, extend the image with a Dockerfile and use the built-in `docker-maven-download` script, which supports downloading from Maven Central, Confluent, and custom repos with MD5 verification.

### Debezium ↔ Kafka broker compatibility

| Debezium version | Kafka Connect / Broker tested |
|---|---|
| 3.0.0.Final | 3.8.0 |
| 3.0.2–3.0.8 | 3.9.0 |
| 3.1.x | 3.9.0 |
| 3.2.x | 4.0.0 |
| 3.3.x | 4.1.0 |
| 3.4.x | 4.1.1 |

Best matches for target brokers:

| Kafka broker | Debezium image |
|---|---|
| 3.8 | `quay.io/debezium/connect:3.0` (3.0.0.Final) |
| 3.9 | `quay.io/debezium/connect:3.1` |
| 4.0.1 | `quay.io/debezium/connect:3.2` |

Release notes note: *"See the Kafka documentation for compatibility with other versions of Kafka brokers."* — Kafka protocol compatibility generally allows a Connect worker to talk to brokers a few minor versions apart.

### Implication for PoC1

Since PoC1 controls both the broker and Connect in Compose, we can pin matching versions. The `debezium/connect` image already bundles a JDBC connector, so a custom Dockerfile may not be needed unless we specifically require the Confluent JDBC Sink (which is a different connector).

### Sources

- [debezium/connect — Docker Hub](https://hub.docker.com/r/debezium/connect)
- [connect/2.7/Dockerfile — GitHub](https://github.com/debezium/container-images/blob/main/connect/2.7/Dockerfile)
- [connect-base/2.7/Dockerfile — GitHub](https://github.com/debezium/container-images/blob/main/connect-base/2.7/Dockerfile)
- [Installing Debezium — Official Docs](https://debezium.io/documentation/reference/stable/install.html)
- [Debezium Releases Overview](https://debezium.io/releases/)
- [Debezium 3.0 Release Notes](https://debezium.io/releases/3.0/release-notes)
- [Debezium 3.1 Release Notes](https://debezium.io/releases/3.1/release-notes)
- [Debezium 3.2 Release Series](https://debezium.io/releases/3.2/)
- [Debezium 3.3 Final Released](https://debezium.io/blog/2025/10/01/debezium-3-3-final-released/)
- [Debezium 3.4 Release Notes](https://debezium.io/releases/3.4/release-notes)

---

## 2. Postgres Source DB Requirements for Debezium CDC

**Date:** 2026-01-30
**Objective:** Document the PostgreSQL configuration required for Debezium CDC and evaluate the logical decoding plugin options.

### WAL level

Debezium's PostgreSQL connector requires `wal_level=logical` on the source database. This enables logical decoding, which translates WAL (Write-Ahead Log) entries into a stream of row-level change events. The default `wal_level` in PostgreSQL is `replica`, which only supports physical replication.

In Docker Compose, this is set via command args:
```yaml
command: ["-c", "wal_level=logical"]
```

This avoids the need for a custom `postgresql.conf` file.

### Logical decoding plugins

| Plugin | Ships with Postgres | Extra install | Output format |
|---|---|---|---|
| `pgoutput` | Yes (10+) | No | Protocol-native |
| `decoderbufs` | No | Yes (C extension) | Protobuf |

Debezium supports both, but **`pgoutput` is strongly recommended**:
- Built into PostgreSQL 10+ — zero setup, no extension to compile or install.
- Used by PostgreSQL's native logical replication, so it is well-maintained.
- `decoderbufs` was historically needed before `pgoutput` support was added to Debezium, but is now considered legacy.
- The Debezium docs default all examples to `pgoutput` as of Debezium 2.x+.

### Replication slots

Debezium automatically creates a replication slot on first connection (default name: `debezium`). The slot ensures WAL segments are retained until the connector has consumed them. Key considerations:
- If the connector is stopped for an extended period, WAL accumulates on disk.
- The slot name is configurable via `slot.name` in the connector config.
- Dropping and recreating a connector without cleaning up the slot can cause WAL bloat.

### Publication

With `pgoutput`, Debezium uses PostgreSQL's publication mechanism. By default it creates a publication named `dbz_publication` for all tables. This can be scoped to specific tables via `publication.name` and `publication.autocreate.mode` in the connector config.

### Sources

- [Debezium PostgreSQL Connector — Setting up PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#setting-up-postgresql)
- [PostgreSQL Documentation — Logical Decoding](https://www.postgresql.org/docs/16/logicaldecoding.html)
- [Debezium PostgreSQL Connector — pgoutput](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-pgoutput)

---

## 3. PostgreSQL REPLICA IDENTITY and the "before" Image

**Date:** 2026-02-03
**Objective:** Explain the `ALTER TABLE ... REPLICA IDENTITY FULL;` statement and why it is required for complete change event capture with Debezium.

### What is REPLICA IDENTITY?

`REPLICA IDENTITY` is a PostgreSQL table-level setting that controls which column values are written to the WAL (Write-Ahead Log) for UPDATE and DELETE operations during logical replication. It determines what information is available to logical decoding consumers like Debezium.

### REPLICA IDENTITY options

| Option | Behavior | WAL content on UPDATE/DELETE |
|---|---|---|
| `DEFAULT` | Only primary key columns are logged | PK values only |
| `FULL` | All column values are logged | Full row (before image) |
| `NOTHING` | No row identity is logged | Nothing (cannot replicate) |
| `USING INDEX <index>` | Columns in the specified unique index are logged | Index column values |

The default setting is `DEFAULT`, which only writes primary key values to the WAL.

### Why Debezium needs REPLICA IDENTITY FULL

Debezium change events contain a `before` field that represents the row state prior to the change. This is critical for:

1. **DELETE events** — Without `FULL`, the `before` field only contains the primary key. Consumers cannot know what data was deleted.
2. **UPDATE events** — Without `FULL`, the `before` field only contains the primary key. Consumers cannot determine which columns changed or implement idempotent replay.
3. **Sink connectors** — The Debezium JDBC Sink uses the `before` image to construct proper `WHERE` clauses for updates and deletes on the target database.

With `REPLICA IDENTITY DEFAULT`:
```json
{
  "before": { "id": 42 },
  "after": { "id": 42, "customer_name": "Alice", "product": "Widget", "quantity": 10 }
}
```

With `REPLICA IDENTITY FULL`:
```json
{
  "before": { "id": 42, "customer_name": "Alice", "product": "Widget", "quantity": 5 },
  "after": { "id": 42, "customer_name": "Alice", "product": "Widget", "quantity": 10 }
}
```

### Trade-offs

Setting `REPLICA IDENTITY FULL` increases WAL volume because every UPDATE and DELETE writes all column values (not just the PK). For tables with many columns or frequent updates, this can:
- Increase disk I/O on the source database
- Increase replication slot lag if the connector falls behind
- Increase network traffic to Kafka

For most CDC use cases, the additional overhead is acceptable and the benefits of having complete change events outweigh the costs.

### When to use USING INDEX

If a table has no primary key but has a unique index, `REPLICA IDENTITY USING INDEX` can be used as a middle ground. However, `FULL` is generally preferred for Debezium because:
- It guarantees all columns are available for change detection
- It works regardless of table structure changes
- The Debezium JDBC Sink benefits from having the complete before image

### Sources

- [PostgreSQL Documentation — ALTER TABLE ... REPLICA IDENTITY](https://www.postgresql.org/docs/16/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY)
- [Debezium PostgreSQL Connector — Replica Identity](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-replica-identity)
- [Debezium Blog — Understanding Replica Identity](https://debezium.io/blog/2020/09/03/debezium-1-3-beta1-released/#replica-identity)

---

## 4. Debezium JDBC Sink vs Confluent JDBC Sink

**Date:** 2026-01-30
**Objective:** Compare the two JDBC sink connectors and justify the choice for PoC1.

### Confluent JDBC Sink Connector

- Maintained by Confluent under the Confluent Community License.
- Mature, widely deployed, well-documented.
- **Not bundled** in the Debezium Connect image — would require downloading the Confluent Hub client or manually adding JARs, plus managing license compliance.
- Supports auto-create and auto-evolve for destination tables.
- Designed for generic Kafka Connect use, not specifically for Debezium change events.

### Debezium JDBC Sink Connector

- Maintained by the Debezium project under the Apache 2.0 license.
- **Bundled** in `quay.io/debezium/connect` — zero extra setup.
- Purpose-built to consume Debezium change events (understands `before`/`after` envelope structure natively).
- Supports `schema.evolution=basic` for auto-creating and evolving destination tables.
- Supports upsert (`insert.mode=upsert`) and delete propagation.
- Newer than Confluent's connector but actively developed and tested as part of the Debezium ecosystem.

### Decision rationale

For PoC1, the Debezium JDBC Sink is the better fit:
1. **Zero friction** — already in the image, no extra download or Dockerfile customization.
2. **Native Debezium integration** — understands Debezium's envelope format without extra SMTs.
3. **License simplicity** — Apache 2.0, no Confluent Community License considerations.
4. **Ecosystem consistency** — source and sink connectors from the same project, same release cadence.

### Sources

- [Debezium JDBC Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/jdbc.html)
- [Confluent JDBC Connector Documentation](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html)
- [Debezium Connect Image Contents](https://quay.io/repository/debezium/connect)
