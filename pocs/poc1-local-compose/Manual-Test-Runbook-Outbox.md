# Manual Test Runbook — Outbox Pattern

Step-by-step manual tests for the transactional outbox pattern. Each test is
self-contained with a command, expected output description, and what to look for.

**Pattern under test:**

```
[Application] ─── BEGIN ──► [INSERT inventory.orders]
                           ► [INSERT inventory.outbox]
               ─── COMMIT ─►

[Debezium source] → [EventRouter SMT] → [Kafka topic per aggregate type]
```

The outbox pattern guarantees that domain events are published to Kafka
atomically with business data changes. An application writes both the business
row and an event row to the `inventory.outbox` table in the same transaction.
Debezium captures the outbox insert and the EventRouter SMT:

- Extracts the `payload` column as the Kafka message value
- Sets the Kafka message key to the `aggregateid` column
- Routes the event to a topic named `outbox.event.<aggregatetype>`
- Adds the `type` column as a Kafka header (`eventType`)

**Connector used:**

| Connector | Config file | Role |
|---|---|---|
| `source-debezium-outbox` | `connectors/source-debezium-outbox.json` | Captures outbox inserts + applies EventRouter SMT |

**Outbox table schema** (created by `sql/source-init.sql`):

| Column | Type | Purpose |
|---|---|---|
| `id` | `uuid` | Unique event ID (becomes a Kafka header) |
| `aggregatetype` | `varchar(255)` | Aggregate root type — determines the target Kafka topic |
| `aggregateid` | `varchar(255)` | Aggregate root ID — becomes the Kafka message key |
| `type` | `varchar(255)` | Event type (e.g., `OrderCreated`) — added as Kafka header |
| `payload` | `jsonb` | Event body — becomes the Kafka message value |
| `created_at` | `timestamptz` | Timestamp for observability (not used by the SMT) |

All commands assume you are in the `pocs/poc1-local-compose/` directory and the
stack is running (`docker compose up -d`).

---

## 0. Prerequisites — verify the stack is healthy

### 0.1 All containers running

```bash
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
```

All six containers should show `Up`. If `kafka-connect` is restarting, wait —
it retries until Kafka is ready.

### 0.2 Kafka Connect REST API responding

```bash
curl -s http://localhost:8083/ | jq .
```

Expected: JSON with `version`, `commit`, and `kafka_cluster_id` fields.

### 0.3 Verify the outbox table exists

The outbox table is created by `sql/source-init.sql` on first database
initialization. If the stack was previously running without the outbox table,
you need a full stack reset first:

```bash
docker compose down -v && docker compose up -d
```

Verify the table exists:

```bash
docker exec source-db psql -U postgres -d source -c \
  "SELECT column_name, data_type FROM information_schema.columns
   WHERE table_schema = 'inventory' AND table_name = 'outbox'
   ORDER BY ordinal_position;"
```

Expected: six columns (`id`, `aggregatetype`, `aggregateid`, `type`, `payload`,
`created_at`).

### 0.4 Deploy the outbox connector (if not already deployed)

```bash
curl -s -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @connectors/source-debezium-outbox.json | jq .
```

### 0.5 Outbox connector RUNNING

```bash
curl -s http://localhost:8083/connectors/source-debezium-outbox/status \
  | jq '.connector.state, .tasks[0].state'
```

Expected: both print `"RUNNING"`.

---

## 1. Simple outbox event — OrderCreated

### 1.1 Insert an outbox event

Simulate an application writing a domain event to the outbox table:

```bash
docker exec source-db psql -U postgres -d source -c "
  INSERT INTO inventory.outbox (id, aggregatetype, aggregateid, type, payload)
    VALUES (
      'a1b2c3d4-0001-0001-0001-000000000001',
      'Order',
      '1',
      'OrderCreated',
      '{\"order_id\": 1, \"customer_name\": \"Alice\", \"product\": \"Widget A\", \"quantity\": 10}'::jsonb
    );
"
```

### 1.2 Verify the event on the Kafka topic

The EventRouter routes events to `outbox.event.<aggregatetype>`. For
`aggregatetype=Order`, the topic is `outbox.event.Order`.

```bash
docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e
```

Expected: one JSON message containing the payload:

```json
{"order_id": 1, "customer_name": "Alice", "product": "Widget A", "quantity": 10}
```

### 1.3 Verify the Kafka key

The EventRouter sets the Kafka message key to the `aggregateid` column value.

```bash
docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e -K '\t'
```

Expected: output begins with the key `1` followed by a tab and the payload:

```
1	{"order_id": 1, "customer_name": "Alice", ...}
```

### 1.4 Verify event headers

The connector is configured to add the `type` column as a Kafka header named
`eventType` (via `table.fields.additional.placement`).

```bash
docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e -J \
  | jq '{key: .key, headers: .headers, payload: (.payload | fromjson)}'
```

Expected: headers contain `"eventType": "OrderCreated"` and key is `"1"`.

---

## 2. Topic routing — different aggregate types

The EventRouter creates a separate Kafka topic per aggregate type. Events with
different `aggregatetype` values are routed to different topics.

### 2.1 Insert a Customer event

```bash
docker exec source-db psql -U postgres -d source -c "
  INSERT INTO inventory.outbox (id, aggregatetype, aggregateid, type, payload)
    VALUES (
      'a1b2c3d4-0002-0002-0002-000000000002',
      'Customer',
      '42',
      'CustomerUpdated',
      '{\"customer_id\": 42, \"name\": \"Alice\", \"email\": \"alice@example.com\"}'::jsonb
    );
"
```

### 2.2 Verify on the Customer topic

```bash
docker exec kcat kcat -b kafka:9092 -t outbox.event.Customer -C -e -K '\t'
```

Expected: key `42`, payload with the customer data.

### 2.3 Verify the Order topic was NOT affected

```bash
docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e -q -c
```

Expected: still `1` (only the event from test 1). The Customer event should
not appear on the Order topic.

### 2.4 Insert a Payment event (third aggregate type)

```bash
docker exec source-db psql -U postgres -d source -c "
  INSERT INTO inventory.outbox (id, aggregatetype, aggregateid, type, payload)
    VALUES (
      'a1b2c3d4-0003-0003-0003-000000000003',
      'Payment',
      '100',
      'PaymentReceived',
      '{\"payment_id\": 100, \"order_id\": 1, \"amount\": 49.99, \"currency\": \"USD\"}'::jsonb
    );
"
```

### 2.5 Verify three separate topics exist

```bash
docker exec kcat kcat -b kafka:9092 -L \
  | grep "topic \"" | sed 's/.*topic "\(.*\)".*/\1/' | sort | grep "^outbox.event"
```

Expected:

```
outbox.event.Customer
outbox.event.Order
outbox.event.Payment
```

---

## 3. Transactional outbox — atomicity with business data

The core value of the outbox pattern: the business write and the event write
happen in the same database transaction, guaranteeing both succeed or both fail.

This test also deploys the CDC connectors so we can verify the business data
propagates through the CDC pipeline while the outbox event propagates through
the outbox pipeline.

### 3.1 Deploy CDC connectors (if not already deployed)

```bash
curl -s -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @connectors/source-debezium-cdc.json | jq .

curl -s -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @connectors/sink-jdbc-orders.json | jq .
```

Verify both are running:

```bash
curl -s http://localhost:8083/connectors/source-debezium-cdc/status | jq '.connector.state, .tasks[0].state'
curl -s http://localhost:8083/connectors/sink-jdbc-orders/status | jq '.connector.state, .tasks[0].state'
```

### 3.2 Insert order + outbox event in one transaction

```bash
docker exec source-db psql -U postgres -d source -c "
  BEGIN;
  INSERT INTO inventory.orders (customer_name, product, quantity)
    VALUES ('Eve', 'Widget E', 3);
  INSERT INTO inventory.outbox (id, aggregatetype, aggregateid, type, payload)
    VALUES (
      'a1b2c3d4-0004-0004-0004-000000000004',
      'Order',
      '4',
      'OrderCreated',
      '{\"order_id\": 4, \"customer_name\": \"Eve\", \"product\": \"Widget E\", \"quantity\": 3}'::jsonb
    );
  COMMIT;
"
```

### 3.3 Verify the order arrived in the sink (via CDC pipeline)

```bash
docker exec sink-db psql -U postgres -d sink -c \
  "SELECT id, customer_name, product, quantity FROM inventory.orders
   WHERE customer_name = 'Eve';"
```

Expected: Eve's row is present in the sink.

### 3.4 Verify the outbox event arrived on Kafka (via outbox pipeline)

```bash
docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e \
  | grep "Eve"
```

Expected: an event containing `"customer_name": "Eve"`.

### 3.5 Second transactional insert

```bash
docker exec source-db psql -U postgres -d source -c "
  BEGIN;
  INSERT INTO inventory.orders (customer_name, product, quantity)
    VALUES ('Frank', 'Gadget F', 15);
  INSERT INTO inventory.outbox (id, aggregatetype, aggregateid, type, payload)
    VALUES (
      'a1b2c3d4-0005-0005-0005-000000000005',
      'Order',
      '5',
      'OrderCreated',
      '{\"order_id\": 5, \"customer_name\": \"Frank\", \"product\": \"Gadget F\", \"quantity\": 15}'::jsonb
    );
  COMMIT;
"
```

Verify both pipelines:

```bash
# CDC: Frank in sink
docker exec sink-db psql -U postgres -d sink -c \
  "SELECT id, customer_name FROM inventory.orders WHERE customer_name = 'Frank';"

# Outbox: Frank on topic
docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e | grep -c "Frank"
```

Expected: Frank's row in the sink, and at least one outbox event mentioning Frank.

---

## 4. CDC isolation — outbox table excluded from CDC connector

The CDC connector (`source-debezium-cdc`) is configured with
`table.exclude.list=inventory.outbox` so it does NOT capture outbox table
changes. Outbox events should only appear on `outbox.event.*` topics via the
outbox connector.

### 4.1 Check that no CDC topic exists for the outbox table

```bash
docker exec kcat kcat -b kafka:9092 -L \
  | grep "topic \"" | sed 's/.*topic "\(.*\)".*/\1/' | sort | grep outbox
```

Expected: only `outbox.event.*` topics appear. There should be NO topic named
`poc1.inventory.outbox`.

### 4.2 Verify publications are separate

```bash
docker exec source-db psql -U postgres -d source -c \
  "SELECT pubname, tablename FROM pg_publication_tables ORDER BY pubname, tablename;"
```

Expected: two publications:
- `dbz_outbox_publication` → `inventory.outbox`
- `dbz_publication` → `inventory.orders` (no outbox table)

---

## 5. Multiple events for the same aggregate

Events for the same aggregate share the same Kafka key (`aggregateid`), which
means they land on the same partition and maintain ordering.

### 5.1 Insert lifecycle events for order 1

```bash
docker exec source-db psql -U postgres -d source -c "
  INSERT INTO inventory.outbox (id, aggregatetype, aggregateid, type, payload) VALUES
    ('a1b2c3d4-0006-0006-0006-000000000006', 'Order', '1',
     'OrderShipped',
     '{\"order_id\": 1, \"shipped_at\": \"2026-02-02T10:00:00Z\"}'::jsonb),
    ('a1b2c3d4-0007-0007-0007-000000000007', 'Order', '1',
     'OrderDelivered',
     '{\"order_id\": 1, \"delivered_at\": \"2026-02-02T14:00:00Z\"}'::jsonb);
"
```

### 5.2 Verify events with the same key arrive in order

```bash
docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e -K '\t' \
  | grep "^1	"
```

Expected: multiple events with key `1`. The events should appear in insertion
order: `OrderCreated`, then `OrderShipped`, then `OrderDelivered`.

### 5.3 Verify event types via headers

```bash
docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e -J \
  | jq 'select(.key == "1") | {key: .key, eventType: (.headers | to_entries[] | select(.key == "eventType") | .value)}'
```

Expected: three events for key `1`, with `eventType` values `OrderCreated`,
`OrderShipped`, and `OrderDelivered`.

---

## 6. Monitoring and health checks

### 6.1 Outbox connector status

```bash
curl -s http://localhost:8083/connectors/source-debezium-outbox/status | jq .
```

Check `connector.state` and `tasks[0].state` are both `RUNNING`.

### 6.2 Connector task details (useful for debugging failures)

```bash
curl -s http://localhost:8083/connectors/source-debezium-outbox/status | jq '.tasks[]'
```

If a task is `FAILED`, the `trace` field contains the Java stack trace.

### 6.3 All connectors at a glance

```bash
curl -s http://localhost:8083/connectors?expand=status | \
  jq '.[] | {name: .status.name, state: .status.connector.state, task_state: .status.tasks[0].state}'
```

### 6.4 List outbox-related Kafka topics

```bash
docker exec kcat kcat -b kafka:9092 -L \
  | grep "topic \"" | sed 's/.*topic "\(.*\)".*/\1/' | sort | grep "^outbox.event"
```

Shows all topics created by the EventRouter. One topic per distinct
`aggregatetype` value.

### 6.5 Event count per outbox topic

```bash
for topic in $(docker exec kcat kcat -b kafka:9092 -L \
  | grep "topic \"" | sed 's/.*topic "\(.*\)".*/\1/' | grep "^outbox.event"); do
  count=$(docker exec kcat kcat -b kafka:9092 -t "$topic" -C -e -q -c 2>/dev/null)
  echo "$topic: $count"
done
```

### 6.6 PostgreSQL replication slot status

```bash
docker exec source-db psql -U postgres -d source -c \
  "SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn
   FROM pg_replication_slots
   WHERE slot_name = 'debezium_outbox';"
```

Expected: slot `debezium_outbox`, plugin `pgoutput`, `active = t`.

### 6.7 Outbox table row count

In a real application, the outbox table would be periodically cleaned up. For
this PoC, rows accumulate:

```bash
docker exec source-db psql -U postgres -d source -c \
  "SELECT count(*) AS total,
          count(DISTINCT aggregatetype) AS aggregate_types,
          count(DISTINCT aggregateid) AS aggregates
   FROM inventory.outbox;"
```

### 6.8 Kafka Connect logs (outbox connector)

```bash
# Last 50 lines
docker logs kafka-connect --tail 50

# Filter for outbox connector activity
docker logs kafka-connect 2>&1 | grep -i "outbox"

# Filter for errors
docker logs kafka-connect 2>&1 | grep -i "ERROR"
```

### 6.9 WAL retention for the outbox slot

```bash
docker exec source-db psql -U postgres -d source -c \
  "SELECT slot_name,
          pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
   FROM pg_replication_slots
   WHERE slot_name = 'debezium_outbox';"
```

---

## 7. Recovery tests

### 7.1 Restart Kafka Connect — verify no event loss

```bash
# Count events before
docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e -q -c

# Restart Connect
docker compose restart kafka-connect

# Wait for connector to resume
sleep 15

# Insert an outbox event while (or after) Connect restarts
docker exec source-db psql -U postgres -d source -c "
  INSERT INTO inventory.outbox (id, aggregatetype, aggregateid, type, payload)
    VALUES (
      'a1b2c3d4-0008-0008-0008-000000000008',
      'Order',
      '99',
      'OrderCreated',
      '{\"order_id\": 99, \"customer_name\": \"Recovery Test\"}'::jsonb
    );
"

# Wait for propagation
sleep 5

# Verify the event arrived
docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e | grep "Recovery Test"
```

Expected: the event is present. Kafka Connect resumes from the last committed
offset — no event loss.

### 7.2 Restart source database — verify connector recovers

```bash
docker compose restart source-db

# Wait for Postgres to accept connections
sleep 10

# Insert after restart
docker exec source-db psql -U postgres -d source -c "
  INSERT INTO inventory.outbox (id, aggregatetype, aggregateid, type, payload)
    VALUES (
      'a1b2c3d4-0009-0009-0009-000000000009',
      'Order',
      '100',
      'OrderCreated',
      '{\"order_id\": 100, \"customer_name\": \"DB Restart Test\"}'::jsonb
    );
"

# Wait for propagation
sleep 5

docker exec kcat kcat -b kafka:9092 -t outbox.event.Order -C -e | grep "DB Restart Test"
```

Expected: the event is present. The outbox connector reconnects automatically
using the `debezium_outbox` replication slot, which retains WAL across restarts.

---

## 8. Cleanup / reset

### 8.1 Delete all outbox events (keep connector running)

```bash
docker exec source-db psql -U postgres -d source -c "DELETE FROM inventory.outbox;"
```

> **Note:** Deleting outbox rows does NOT produce events on Kafka. The
> EventRouter only processes `INSERT` operations; deletes on the outbox table
> are ignored.

### 8.2 Delete the outbox connector

```bash
curl -s -X DELETE http://localhost:8083/connectors/source-debezium-outbox
```

### 8.3 Full stack reset (destroy everything)

```bash
docker compose down -v
```

This removes all containers, networks, and volumes. The next `docker compose up -d`
starts from scratch (fresh databases, no connectors registered, no Kafka data).

> **Note:** Outbox Kafka topics (`outbox.event.*`) are automatically recreated
> when the outbox connector processes new events.
