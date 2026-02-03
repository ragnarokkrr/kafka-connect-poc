# PoC1 — Local Compose

End-to-end Change Data Capture pipeline using Kafka Connect, running entirely
in Docker Compose on a single machine.

## Overview

```
┌────────────┐      ┌──────────────┐      ┌───────┐      ┌──────────────┐      ┌────────────┐
│ source-db  │─CDC─▶│  Debezium    │─────▶│ Kafka │─────▶│  JDBC Sink   │─────▶│  sink-db   │
│ Postgres   │      │  Source      │      │       │      │  Connector   │      │  Postgres  │
└────────────┘      └──────────────┘      └───────┘      └──────────────┘      └────────────┘
```

A row inserted into `source-db.inventory.orders` is captured by the Debezium
PostgreSQL source connector, published to a Kafka topic, and written to
`sink-db` by the Debezium JDBC sink connector.

## Prerequisites

- Docker Engine 24+
- Docker Compose v2 (`docker compose` subcommand)

## Quick start

```bash
# Start all services in the background
docker compose up -d

# Verify all six containers are running
docker compose ps

# Check Kafka Connect is ready (may take 30-60 s on first start)
curl -s http://localhost:8083/ | jq .
```

## Services

| Service | Image | Host port | Purpose |
|---|---|---|---|
| `source-db` | `postgres:16` | 5432 | Source Postgres with `wal_level=logical` |
| `sink-db` | `postgres:16` | 5433 | Sink Postgres (vanilla) |
| `kafka` | `apache/kafka:4.0.0` | 9092 | Single-node KRaft broker+controller |
| `kafka-connect` | `quay.io/debezium/connect:3.2` | 8083 | Kafka Connect with Debezium plugins |
| `kafka-ui` | `provectuslabs/kafka-ui:latest` | 8080 | Web UI for Kafka topics, consumers, and connectors |
| `kcat` | `edenhill/kcat:1.7.1` | — | Lightweight Kafka CLI client (idle; use via `docker exec`) |

## Architecture

- **Kafka** runs Apache Kafka 4.0 in KRaft combined mode (broker + controller
  in one JVM). No ZooKeeper required.
- **Kafka Connect** uses the Debezium all-in-one image, which bundles both the
  PostgreSQL source connector and the JDBC sink connector.
- **Serialization** is JSON with embedded schemas (`schemas.enable=true`).
  Each Kafka message wraps the payload in a `{"schema": ..., "payload": ...}`
  envelope. No Schema Registry needed — schemas travel inline with the data.
- **source-db** starts with `wal_level=logical` so Debezium can use the
  `pgoutput` logical decoding plugin (built into Postgres 10+). The
  `inventory.orders` table uses `REPLICA IDENTITY FULL` so that Debezium
  populates the `before` field with all column values on updates and deletes.
  See [Configuring wal_level](#configuring-wal_level) for details on how this
  is set and what roles Debezium needs.
- **sink-db** starts with the `inventory` schema and a pre-created `orders`
  table mirroring the source. The table is pre-created for explicitness;
  `schema.evolution=basic` could auto-create it since schemas are embedded.
- **Kafka UI** provides a web interface at [http://localhost:8080](http://localhost:8080)
  for browsing topics, consumer groups, and managing connectors.
- **kcat** is an idle container with a lightweight Kafka CLI. Use it via
  `docker exec` to list topics, produce, or consume messages:
  ```bash
  docker exec kcat kcat -b kafka:9092 -L          # list topics
  docker exec kcat kcat -b kafka:9092 -t TOPIC -C -e  # consume
  ```

For detailed config property descriptions, Kafka message payload examples, and
sink processing logic, see [Connector-Reference.md](Connector-Reference.md).
For step-by-step manual tests, see [Manual-Test-Runbook.md](Manual-Test-Runbook.md).

## Configuring wal_level

Debezium CDC requires `wal_level=logical` on the source PostgreSQL instance.
This is a `postmaster`-class parameter — it can only take effect at server
startup.

### How this PoC sets it

In Docker, the cleanest approach is a command-line argument:

```yaml
# docker-compose.yml
source-db:
  image: postgres:16
  command: ["-c", "wal_level=logical"]
```

This is active on the very first boot. No restart needed.

### Can it be set via SQL?

Yes, but it still requires a restart:

```sql
-- Writes to postgresql.auto.conf (persisted across restarts)
ALTER SYSTEM SET wal_level = logical;

-- A reload is NOT enough — wal_level is postmaster-class
SELECT pg_reload_conf();  -- does NOT apply wal_level

-- A full server restart is required
-- (in Docker: docker compose restart source-db)
```

This makes `ALTER SYSTEM` impractical for Docker init scripts
(`/docker-entrypoint-initdb.d/`), since those run during the first startup
and the new value wouldn't be active until a second startup.

### Verify the current wal_level

```bash
docker exec source-db psql -U postgres -d source -c "SHOW wal_level;"
```

Expected output: `logical`.

### Required PostgreSQL roles for Debezium

In this PoC we use the `postgres` superuser for simplicity. In a real
environment, Debezium needs a dedicated user with specific privileges:

| Privilege | Purpose | Grant statement |
|---|---|---|
| `LOGIN` | Connect to the database | `CREATE ROLE debezium WITH LOGIN PASSWORD '...'` |
| `REPLICATION` | Create and use replication slots | `ALTER ROLE debezium REPLICATION;` |
| `SELECT` on tables | Read table data during snapshot | `GRANT SELECT ON ALL TABLES IN SCHEMA inventory TO debezium;` |
| `CREATE` on database | Auto-create publications (if `publication.autocreate.mode != disabled`) | `GRANT CREATE ON DATABASE source TO debezium;` |

Minimal setup for a non-superuser:

```sql
-- Create the Debezium user
CREATE ROLE debezium WITH LOGIN PASSWORD 'dbz_password' REPLICATION;

-- Grant read access to captured tables
GRANT USAGE ON SCHEMA inventory TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA inventory TO debezium;

-- Allow publication creation (needed for pgoutput)
GRANT CREATE ON DATABASE source TO debezium;
```

> **Note:** The `postgres` superuser used in this PoC implicitly has all of
> the above. A production deployment should use a least-privilege role.

## Connector setup

### 1. Deploy the Debezium CDC source connector

The config file lives at `connectors/source-debezium-cdc.json`.

```bash
# Register the source connector
curl -s -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @connectors/source-debezium-cdc.json | jq .
```

Verify it is running:

```bash
# Check connector status
curl -s http://localhost:8083/connectors/source-debezium-cdc/status | jq .
```

The connector should show `"state": "RUNNING"` for both the connector and its
task. Once running, it will:

1. Create a replication slot (`debezium_cdc`) and publication (`dbz_publication`)
   on `source-db`.
2. Take an initial snapshot of `inventory.orders` (the 3 seed rows).
3. Stream all subsequent inserts, updates, and deletes to Kafka.

#### Topic naming

Debezium uses the pattern `<topic.prefix>.<schema>.<table>`. With the current
config, the orders table produces to:

```
poc1.inventory.orders
```

#### Verify CDC events

```bash
# List topics — the orders topic should appear after connector starts
docker exec kcat kcat -b kafka:9092 -L | grep poc1

# Consume all events from the orders topic (extract Debezium envelope from schema wrapper)
docker exec kcat kcat -b kafka:9092 -t poc1.inventory.orders -C -e | jq '.payload'
```

Each message has a Debezium envelope with `before`, `after`, `op`, and `source`
fields. The `op` field indicates the operation type:

| `op` | Meaning |
|---|---|
| `r` | Read (initial snapshot) |
| `c` | Create (insert) |
| `u` | Update |
| `d` | Delete |

#### Test a live change

```bash
# Insert a new row into source-db
docker exec source-db psql -U postgres -d source -c \
  "INSERT INTO inventory.orders (customer_name, product, quantity) VALUES ('Dave', 'Gadget D', 7);"

# Consume the new event (should show op=c)
docker exec kcat kcat -b kafka:9092 -t poc1.inventory.orders -C -e \
  | jq '.payload | select(.op == "c")'
```

### 2. Deploy the JDBC sink connector

The config file lives at `connectors/sink-jdbc-orders.json`.

```bash
# Register the sink connector
curl -s -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @connectors/sink-jdbc-orders.json | jq .
```

Verify it is running:

```bash
curl -s http://localhost:8083/connectors/sink-jdbc-orders/status | jq .
```

Once running, the connector will:

1. Subscribe to the `poc1.inventory.orders` topic.
2. Upsert each change event into `inventory.orders` on `sink-db`.
3. Propagate deletes (row removed from source is deleted in sink).

Key config choices:

| Property | Value | Why |
|---|---|---|
| `insert.mode` | `upsert` | Handles both snapshot reads and subsequent inserts/updates idempotently |
| `primary.key.mode` | `record_key` | Uses the Kafka record key (`id`) to match rows |
| `delete.enabled` | `true` | Tombstone events (Debezium `op=d`) delete the sink row |
| `schema.evolution` | `none` | Table is pre-created for explicitness |
| `table.name.format` | `inventory.orders` | Maps topic `poc1.inventory.orders` to the sink table directly |

#### Verify end-to-end replication

```bash
# Check the snapshot rows arrived in the sink
docker exec sink-db psql -U postgres -d sink -c "SELECT * FROM inventory.orders;"
```

The 3 seed rows from `source-db` should appear.

#### Test a live round-trip

```bash
# Insert in source
docker exec source-db psql -U postgres -d source -c \
  "INSERT INTO inventory.orders (customer_name, product, quantity) VALUES ('Eve', 'Widget E', 3);"

# Query sink (may need a second for propagation)
docker exec sink-db psql -U postgres -d sink -c "SELECT * FROM inventory.orders ORDER BY id;"
```

#### Test update and delete propagation

```bash
# Update a row in source
docker exec source-db psql -U postgres -d source -c \
  "UPDATE inventory.orders SET quantity = 99 WHERE customer_name = 'Alice';"

# Delete a row in source
docker exec source-db psql -U postgres -d source -c \
  "DELETE FROM inventory.orders WHERE customer_name = 'Bob';"

# Verify in sink
docker exec sink-db psql -U postgres -d sink -c "SELECT * FROM inventory.orders ORDER BY id;"
```

Alice's quantity should be `99`, and Bob's row should be gone.

### Managing connectors

```bash
# List all connectors
curl -s http://localhost:8083/connectors | jq .

# Pause a connector
curl -s -X PUT http://localhost:8083/connectors/source-debezium-cdc/pause

# Resume a connector
curl -s -X PUT http://localhost:8083/connectors/source-debezium-cdc/resume

# Delete a connector (also drops the replication slot)
curl -s -X DELETE http://localhost:8083/connectors/source-debezium-cdc
```

## Teardown

```bash
# Stop and remove containers, networks
docker compose down

# Also remove persistent volumes (full reset)
docker compose down -v
```
