# Connector Reference — PoC1

Detailed reference for the three connectors used in this PoC: the Debezium
PostgreSQL CDC source, the Debezium Outbox source (with EventRouter SMT), and
the Debezium JDBC sink. Covers every config property, the Kafka message format,
and how each connector transforms data.

---

## 1. Source: Debezium PostgreSQL CDC

**Config file:** `connectors/source-debezium-cdc.json`
**Connector class:** `io.debezium.connector.postgresql.PostgresConnector`

### Config properties

| Property | Value | Description |
|---|---|---|
| `connector.class` | `io.debezium.connector.postgresql.PostgresConnector` | Debezium's Postgres source connector |
| `database.hostname` | `source-db` | Docker service name of the source Postgres |
| `database.port` | `5432` | Postgres port inside the Docker network |
| `database.user` | `postgres` | Database user (needs replication privileges) |
| `database.password` | `postgres` | Database password |
| `database.dbname` | `source` | Database to capture changes from |
| `topic.prefix` | `poc1` | Prefix for all Kafka topics; also serves as the logical server name |
| `schema.include.list` | `inventory` | Only capture tables in the `inventory` schema |
| `plugin.name` | `pgoutput` | Logical decoding plugin (built into Postgres 10+) |
| `slot.name` | `debezium_cdc` | Name of the replication slot created on the source |
| `publication.name` | `dbz_publication` | Name of the Postgres publication for pgoutput |
| `publication.autocreate.mode` | `filtered` | Auto-create publication scoped to `schema.include.list` tables only |
| `key.converter` | `...JsonConverter` | Serialize record keys as JSON |
| `key.converter.schemas.enable` | `true` | Include JSON Schema in keys (required by JDBC sink `record_key` mode) |
| `value.converter` | `...JsonConverter` | Serialize record values as JSON |
| `value.converter.schemas.enable` | `true` | Include JSON Schema in values (provides type info to JDBC sink) |

### Topic naming

Pattern: `<topic.prefix>.<schema>.<table>`

For `inventory.orders` the topic is:

```
poc1.inventory.orders
```

### Reading messages with kcat

With `schemas.enable=true`, each Kafka message is a JSON object with
`schema` and `payload` fields. kcat outputs this raw JSON directly:

```bash
# Full message (schema + payload)
docker exec kcat kcat -b kafka:9092 -t poc1.inventory.orders -C -e | jq .

# Extract just the Debezium envelope (skip the schema)
docker exec kcat kcat -b kafka:9092 -t poc1.inventory.orders -C -e | jq '.payload'

# Extract specific fields
docker exec kcat kcat -b kafka:9092 -t poc1.inventory.orders -C -e \
  | jq '.payload | {op: .op, id: .after.id, customer: .after.customer_name}'

# Filter by operation type
docker exec kcat kcat -b kafka:9092 -t poc1.inventory.orders -C -e \
  | jq '.payload | select(.op == "c")'
```

With `kcat -J`, the output is a kcat metadata envelope where `key` and
`payload` are **JSON-encoded strings**. Use `fromjson` to parse them:

```bash
# -J mode: includes topic, partition, offset metadata
docker exec kcat kcat -b kafka:9092 -t poc1.inventory.orders -C -e -J \
  | jq '.payload | fromjson | .payload'
```

The non-`-J` form is recommended for most use cases — it's simpler and avoids
the double-`.payload` (kcat's vs the schema wrapper's).

### Kafka record key

The record key is a JSON object with an embedded schema and payload.
The payload contains the primary key column(s):

```json
{
  "schema": {
    "type": "struct",
    "fields": [{"type": "int32", "optional": false, "field": "id"}],
    "optional": false,
    "name": "poc1.inventory.orders.Key"
  },
  "payload": {"id": 1}
}
```

### Kafka record value — Debezium envelope

Every change event uses the Debezium envelope format. The structure varies
by operation type.

#### Snapshot read (`op: r`)

Emitted during the initial snapshot for each existing row:

```json
{
  "before": null,
  "after": {
    "id": 1,
    "customer_name": "Alice",
    "product": "Widget A",
    "quantity": 10,
    "created_at": "2026-01-30T19:59:32.893285Z"
  },
  "source": {
    "version": "3.2.4.Final",
    "connector": "postgresql",
    "name": "poc1",
    "ts_ms": 1769803263532,
    "snapshot": "first",
    "db": "source",
    "schema": "inventory",
    "table": "orders",
    "txId": 747,
    "lsn": 26678760
  },
  "transaction": null,
  "op": "r",
  "ts_ms": 1769803263752
}
```

#### Insert (`op: c`)

```json
{
  "before": null,
  "after": {
    "id": 4,
    "customer_name": "Dave",
    "product": "Gadget D",
    "quantity": 7,
    "created_at": "2026-01-30T20:05:12.345678Z"
  },
  "source": {
    "version": "3.2.4.Final",
    "connector": "postgresql",
    "name": "poc1",
    "ts_ms": 1769803512345,
    "snapshot": "false",
    "db": "source",
    "schema": "inventory",
    "table": "orders",
    "txId": 750,
    "lsn": 26679000
  },
  "transaction": null,
  "op": "c",
  "ts_ms": 1769803512400
}
```

#### Update (`op: u`)

With `REPLICA IDENTITY FULL` (set in `sql/source-init.sql`), both `before`
(previous row state) and `after` (new row state) contain all columns:

```json
{
  "before": {
    "id": 1,
    "customer_name": "Alice",
    "product": "Widget A",
    "quantity": 10,
    "created_at": "2026-01-30T19:59:32.893285Z"
  },
  "after": {
    "id": 1,
    "customer_name": "Alice",
    "product": "Widget A",
    "quantity": 99,
    "created_at": "2026-01-30T19:59:32.893285Z"
  },
  "source": { "...": "..." },
  "transaction": null,
  "op": "u",
  "ts_ms": 1769804668214
}
```

> **Note:** Without `REPLICA IDENTITY FULL` (the default), `before` would be
> `null` for updates, and deletes would only contain the PK with type defaults
> in non-PK fields.

#### Delete (`op: d`)

With `REPLICA IDENTITY FULL`, `before` contains all column values of the
deleted row:

```json
{
  "before": {
    "id": 2,
    "customer_name": "Bob",
    "product": "Widget B",
    "quantity": 5,
    "created_at": "2026-01-30T19:59:32.893285Z"
  },
  "after": null,
  "source": { "...": "..." },
  "transaction": null,
  "op": "d",
  "ts_ms": 1769804782617
}
```

After a delete event, Debezium also emits a **tombstone record** — a message
with the same key but a `null` value. This signals log compaction to remove
the key entirely.

### Envelope field reference

| Field | Type | Description |
|---|---|---|
| `before` | object \| null | Row state before the change. `null` for inserts and snapshot reads. With `REPLICA IDENTITY FULL`, contains all columns for updates and deletes. |
| `after` | object \| null | Row state after the change. `null` for deletes. |
| `source` | object | Metadata: connector name, database, schema, table, LSN, transaction ID, snapshot flag. |
| `op` | string | Operation: `r` (read/snapshot), `c` (create), `u` (update), `d` (delete). |
| `transaction` | object \| null | Transaction metadata (id, total order, data collection order). `null` unless transaction metadata topic is enabled. |
| `ts_ms` | long | Timestamp (ms since epoch) when the connector processed the event. |

### Column type mapping

| Postgres type | JSON representation | Notes |
|---|---|---|
| `serial` / `int` | number | Plain integer |
| `text` | string | Plain string |
| `timestamptz` | string | ISO 8601 with microsecond precision (e.g., `"2026-01-30T19:59:32.893285Z"`) |

> The `created_at` field appears as an ISO 8601 string with microsecond
> precision (e.g., `"2026-01-30T20:12:37.716216Z"`). The schema metadata
> carries the logical type `io.debezium.time.ZonedTimestamp`, which the
> JDBC sink uses to write the correct `timestamptz` value to Postgres.

---

## 2. Source: Debezium Outbox (EventRouter)

**Config file:** `connectors/source-debezium-outbox.json`
**Connector class:** `io.debezium.connector.postgresql.PostgresConnector`

This connector uses the same Debezium PostgreSQL source but is configured
specifically for the transactional outbox pattern. The EventRouter SMT
transforms raw CDC events from the `inventory.outbox` table into clean
domain events routed to topic-per-aggregate-type.

### Config properties

| Property | Value | Description |
|---|---|---|
| `connector.class` | `io.debezium.connector.postgresql.PostgresConnector` | Same Debezium Postgres connector as CDC |
| `database.hostname` | `source-db` | Docker service name of the source Postgres |
| `database.port` | `5432` | Postgres port inside the Docker network |
| `database.user` | `postgres` | Database user (needs replication privileges) |
| `database.password` | `postgres` | Database password |
| `database.dbname` | `source` | Database to capture changes from |
| `topic.prefix` | `poc1-outbox` | Prefix for internal Debezium topics (not the routed event topics) |
| `table.include.list` | `inventory.outbox` | Only capture the outbox table |
| `plugin.name` | `pgoutput` | Logical decoding plugin |
| `slot.name` | `debezium_outbox` | Separate replication slot from CDC connector |
| `publication.name` | `dbz_outbox_publication` | Separate publication from CDC connector |
| `publication.autocreate.mode` | `filtered` | Auto-create publication scoped to outbox table only |
| `tombstones.on.delete` | `false` | Outbox rows are never deleted by consumers; no tombstones needed |

### EventRouter SMT properties

| Property | Value | Description |
|---|---|---|
| `transforms` | `outbox` | Name of the transform chain (single transform in this case) |
| `transforms.outbox.type` | `io.debezium.transforms.outbox.EventRouter` | The Debezium Outbox EventRouter SMT |
| `transforms.outbox.route.by.field` | `aggregatetype` | Column that determines the target topic |
| `transforms.outbox.route.topic.replacement` | `outbox.event.${routedByValue}` | Topic pattern; `${routedByValue}` is replaced by `aggregatetype` value |
| `transforms.outbox.table.field.event.id` | `id` | Column used as the event ID (for deduplication) |
| `transforms.outbox.table.field.event.key` | `aggregateid` | Column used as the Kafka record key |
| `transforms.outbox.table.field.event.payload` | `payload` | Column containing the event payload (becomes the record value) |
| `transforms.outbox.table.fields.additional.placement` | `type:header:eventType` | Place `type` column value in Kafka header `eventType` |

### Converters

| Property | Value | Description |
|---|---|---|
| `key.converter` | `StringConverter` | Record key is a plain string (the `aggregateid`) |
| `value.converter` | `StringConverter` | Record value is a plain string (the JSON `payload`) |

Unlike the CDC connector, the outbox connector uses `StringConverter` because
the EventRouter extracts just the `aggregateid` and `payload` values — no
schema wrapper is needed.

### Topic naming

The EventRouter routes each event to a topic based on the `aggregatetype` column:

| `aggregatetype` value | Target topic |
|---|---|
| `Order` | `outbox.event.Order` |
| `Customer` | `outbox.event.Customer` |
| `Shipment` | `outbox.event.Shipment` |

The `poc1-outbox` prefix is only used for internal Debezium topics (offsets,
schema history). Routed events go to `outbox.event.<aggregatetype>`.

### Outbox table schema

The connector expects this table structure (defined in `sql/source-init.sql`):

```sql
CREATE TABLE inventory.outbox (
    id            uuid         NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregatetype varchar(255) NOT NULL,  -- determines target topic
    aggregateid   varchar(255) NOT NULL,  -- becomes Kafka record key
    type          varchar(255) NOT NULL,  -- event type, placed in header
    payload       jsonb,                  -- becomes Kafka record value
    created_at    timestamptz  NOT NULL DEFAULT now()
);
```

### Reading messages with kcat

Outbox messages are plain strings (no schema wrapper), making them simpler
to consume:

```bash
# Read all events from Order aggregate topic
docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e

# Pretty-print JSON payloads
docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e | jq .

# Show key and value together
docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e -K ': '

# Show headers (includes eventType)
docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e -f 'Key: %k\nHeaders: %h\nValue: %s\n---\n'
```

### Kafka record structure

#### Record key

Plain string containing the `aggregateid` value:

```
order-123
```

#### Record value

Plain JSON string containing the `payload` column value:

```json
{
  "orderId": "order-123",
  "customerId": "cust-456",
  "items": [
    {"sku": "WIDGET-A", "qty": 2},
    {"sku": "GADGET-B", "qty": 1}
  ],
  "total": 150.00
}
```

#### Record headers

| Header | Source | Example value |
|---|---|---|
| `eventType` | `type` column | `OrderCreated` |
| `id` | `id` column (UUID) | `550e8400-e29b-41d4-a716-446655440000` |

### Transformation flow

```
inventory.outbox row                          Kafka message
────────────────────                          ─────────────

id:            550e8400-...       ───────►    header: id = "550e8400-..."
aggregatetype: Order              ───────►    topic: outbox.event.Order
aggregateid:   order-123          ───────►    key: "order-123"
type:          OrderCreated       ───────►    header: eventType = "OrderCreated"
payload:       {"orderId":...}    ───────►    value: {"orderId":...}
created_at:    2026-01-30T...                 (not included in message)
```

### Why use the Outbox pattern?

1. **Transactional consistency** — The outbox row is inserted in the same
   transaction as the business data change. Either both succeed or both fail.

2. **Decoupling** — Applications don't need Kafka client libraries. They just
   write to a database table.

3. **Clean event schema** — The EventRouter strips away the Debezium envelope,
   producing events with just the domain payload.

4. **Topic-per-aggregate** — Events are automatically routed to the correct
   topic based on the aggregate type, enabling per-aggregate consumers.

### Comparison with CDC connector

| Aspect | CDC Connector | Outbox Connector |
|---|---|---|
| Captures | All changes to `inventory.orders` | Only inserts to `inventory.outbox` |
| Message format | Debezium envelope with `before`/`after` | Clean domain event (just payload) |
| Record key | Primary key struct with schema | Plain aggregate ID string |
| Topic naming | `poc1.inventory.orders` (table-based) | `outbox.event.<aggregatetype>` (field-based) |
| Use case | Data replication, audit log | Domain event publishing |

---

## 3. Sink: Debezium JDBC

**Config file:** `connectors/sink-jdbc-orders.json`
**Connector class:** `io.debezium.connector.jdbc.JdbcSinkConnector`

### Config properties

| Property | Value | Description |
|---|---|---|
| `connector.class` | `io.debezium.connector.jdbc.JdbcSinkConnector` | Debezium's JDBC sink connector |
| `connection.url` | `jdbc:postgresql://sink-db:5432/sink` | JDBC URL for the sink database |
| `connection.username` | `postgres` | Database user |
| `connection.password` | `postgres` | Database password |
| `topics` | `poc1.inventory.orders` | Kafka topic(s) to consume from |
| `insert.mode` | `upsert` | `INSERT ... ON CONFLICT DO UPDATE` — idempotent writes |
| `primary.key.mode` | `record_key` | Derive PK from the Kafka record key |
| `primary.key.fields` | `id` | Column(s) used as the primary key |
| `delete.enabled` | `true` | Process tombstone records as `DELETE` statements |
| `schema.evolution` | `none` | Table must exist; no auto-create or alter |
| `table.name.format` | `inventory.orders` | Target table (overrides default topic-name-based mapping) |
| `key.converter` | `...JsonConverter` | Deserialize record keys as JSON |
| `key.converter.schemas.enable` | `true` | Keys include JSON Schema (required for `record_key` PK mode) |
| `value.converter` | `...JsonConverter` | Deserialize record values as JSON |
| `value.converter.schemas.enable` | `true` | Values include JSON Schema (provides type info for column mapping) |

### How the sink processes each operation

The Debezium JDBC sink natively understands the Debezium envelope. It reads
the `op` field and the `after`/`before` objects to decide what SQL to execute.

| Source `op` | Sink action | SQL equivalent |
|---|---|---|
| `r` (snapshot read) | Upsert | `INSERT INTO inventory.orders (...) VALUES (...) ON CONFLICT (id) DO UPDATE SET ...` |
| `c` (insert) | Upsert | Same as above |
| `u` (update) | Upsert | Same as above (uses `after` values) |
| `d` (delete) | Delete | `DELETE FROM inventory.orders WHERE id = ...` |
| Tombstone (`null` value) | Skipped | No action (the `d` event already handled the delete) |

### Why `upsert` instead of `insert`

During the initial snapshot, the source emits `op=r` events. If the sink
connector is restarted and replays events, `upsert` prevents duplicate-key
errors. It also handles `op=u` (update) events without needing a separate
`UPDATE` path.

### Why `schema.evolution=none`

Although schemas are embedded (`schemas.enable=true`) and would support
`schema.evolution=basic` for auto-creating tables, we pre-create the target
table in `sql/sink-init.sql` for explicitness. This makes the sink schema
visible in version control rather than implicit in the connector behavior.

### Table mapping

The topic `poc1.inventory.orders` is mapped to the table `inventory.orders`
in the sink database via the `table.name.format` property. Without this
override, the connector would attempt to write to a table literally named
`poc1.inventory.orders`.

### Data flow summary

```
source-db                Kafka                          sink-db
─────────               ──────                         ────────

INSERT INTO             ┌──────────────────────┐
inventory.orders  ───►  │ poc1.inventory.orders │
                        │                      │
                        │  key:   {"id": 4}    │
                        │  value: {             │  ───►  INSERT INTO inventory.orders (...)
                        │    "op": "c",         │        VALUES (...)
                        │    "after": {         │        ON CONFLICT (id) DO UPDATE SET ...
                        │      "id": 4,         │
                        │      "customer_name": │
                        │        "Dave", ...    │
                        │    }                  │
                        │  }                    │
                        └──────────────────────┘

UPDATE                  ┌──────────────────────┐
inventory.orders  ───►  │  key:   {"id": 1}    │
SET quantity=99         │  value: {             │  ───►  INSERT INTO inventory.orders (...)
WHERE id=1              │    "op": "u",         │        VALUES (...)
                        │    "after": {         │        ON CONFLICT (id) DO UPDATE SET
                        │      "id": 1,         │          quantity = 99, ...
                        │      "quantity": 99,  │
                        │      ...              │
                        │    }                  │
                        │  }                    │
                        └──────────────────────┘

DELETE FROM             ┌──────────────────────┐
inventory.orders  ───►  │  key:   {"id": 2}    │
WHERE id=2              │  value: {             │  ───►  DELETE FROM inventory.orders
                        │    "op": "d",         │        WHERE id = 2
                        │    "before": {        │
                        │      "id": 2, ...     │
                        │    },                 │
                        │    "after": null       │
                        │  }                    │
                        └──────────────────────┘
```
