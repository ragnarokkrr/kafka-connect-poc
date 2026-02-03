# Manual Test Runbook — CDC Pipeline

Step-by-step manual tests for the CDC (Change Data Capture) pipeline. Each test
is self-contained with a command, expected output description, and what to look
for.

**Pipeline under test:**

```
[Postgres source-db] → [Debezium Source] → [Kafka] → [JDBC Sink] → [Postgres sink-db]
```

**Connectors used:**

| Connector | Config file | Role |
|---|---|---|
| `source-debezium-cdc` | `connectors/source-debezium-cdc.json` | Captures CDC events from `inventory.orders` |
| `sink-jdbc-orders` | `connectors/sink-jdbc-orders.json` | Writes CDC events to `inventory.orders` on the sink |

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

### 0.3 Deploy CDC connectors (if not already deployed)

```bash
curl -s -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @connectors/source-debezium-cdc.json | jq .

curl -s -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @connectors/sink-jdbc-orders.json | jq .
```

### 0.4 Both connectors RUNNING

```bash
curl -s http://localhost:8083/connectors/source-debezium-cdc/status | jq '.connector.state, .tasks[0].state'
curl -s http://localhost:8083/connectors/sink-jdbc-orders/status | jq '.connector.state, .tasks[0].state'
```

Expected: both print `"RUNNING"` twice (connector + task).

---

## 1. Initial snapshot

The source connector takes a snapshot of existing rows on first start. The
3 seed rows should already be in Kafka and in the sink.

### 1.1 Check source has seed data

```bash
docker exec source-db psql -U postgres -d source -c \
  "SELECT id, customer_name, product, quantity FROM inventory.orders ORDER BY id;"
```

Expected:

```
 id | customer_name | product  | quantity
----+---------------+----------+----------
  1 | Alice         | Widget A |       10
  2 | Bob           | Widget B |        5
  3 | Charlie       | Gadget C |        2
```

### 1.2 Consume snapshot events from Kafka

```bash
docker exec kcat kcat -b kafka:9092 -t poc1.inventory.orders -C -e \
  | jq '.payload | {op: .op, id: .after.id, customer: .after.customer_name}'
```

Expected: 3 events with `"op": "r"` (read = snapshot).

### 1.3 Verify sink received snapshot

```bash
docker exec sink-db psql -U postgres -d sink -c \
  "SELECT id, customer_name, product, quantity FROM inventory.orders ORDER BY id;"
```

Expected: same 3 rows as the source.

---

## 2. INSERT — new row propagation

### 2.1 Insert a row in source

```bash
docker exec source-db psql -U postgres -d source -c \
  "INSERT INTO inventory.orders (customer_name, product, quantity)
   VALUES ('Dave', 'Gadget D', 7);"
```

### 2.2 Verify in source

```bash
docker exec source-db psql -U postgres -d source -c \
  "SELECT id, customer_name, product, quantity FROM inventory.orders WHERE customer_name = 'Dave';"
```

Expected: one row with `id=4` (or next sequence value).

### 2.3 Consume the CDC event

```bash
docker exec kcat kcat -b kafka:9092 -t poc1.inventory.orders -C -e \
  | jq '.payload | select(.op == "c") | {op, after}'
```

Expected: event with `"op": "c"` and `after.customer_name = "Dave"`.

### 2.4 Verify in sink

```bash
docker exec sink-db psql -U postgres -d sink -c \
  "SELECT id, customer_name, product, quantity FROM inventory.orders WHERE customer_name = 'Dave';"
```

Expected: Dave's row present with matching data.

---

## 3. UPDATE — row modification propagation

### 3.1 Update a row in source

```bash
docker exec source-db psql -U postgres -d source -c \
  "UPDATE inventory.orders SET quantity = 99 WHERE customer_name = 'Alice';"
```

### 3.2 Verify in source

```bash
docker exec source-db psql -U postgres -d source -c \
  "SELECT id, customer_name, quantity FROM inventory.orders WHERE customer_name = 'Alice';"
```

Expected: `quantity = 99`.

### 3.3 Consume the CDC event

```bash
docker exec kcat kcat -b kafka:9092 -t poc1.inventory.orders -C -e \
  | jq '.payload | select(.op == "u") | {op, before, after}'
```

Expected: event with `"op": "u"`, `after.quantity = 99`. With
`REPLICA IDENTITY FULL`, `before` contains the previous row state
(with `quantity = 10`).

### 3.4 Verify in sink

```bash
docker exec sink-db psql -U postgres -d sink -c \
  "SELECT id, customer_name, quantity FROM inventory.orders WHERE customer_name = 'Alice';"
```

Expected: `quantity = 99`.

---

## 4. DELETE — row removal propagation

### 4.1 Delete a row in source

```bash
docker exec source-db psql -U postgres -d source -c \
  "DELETE FROM inventory.orders WHERE customer_name = 'Bob';"
```

### 4.2 Verify gone from source

```bash
docker exec source-db psql -U postgres -d source -c \
  "SELECT count(*) FROM inventory.orders WHERE customer_name = 'Bob';"
```

Expected: `0`.

### 4.3 Consume the CDC event

```bash
docker exec kcat kcat -b kafka:9092 -t poc1.inventory.orders -C -e \
  | jq '.payload | select(.op == "d") | {op, before}'
```

Expected: event with `"op": "d"`. With `REPLICA IDENTITY FULL`, `before`
contains all column values of the deleted row.

### 4.4 Verify gone from sink

```bash
docker exec sink-db psql -U postgres -d sink -c \
  "SELECT count(*) FROM inventory.orders WHERE customer_name = 'Bob';"
```

Expected: `0`.

---

## 5. Bulk operations

### 5.1 Multi-row insert

```bash
docker exec source-db psql -U postgres -d source -c \
  "INSERT INTO inventory.orders (customer_name, product, quantity) VALUES
     ('Frank',  'Widget F', 12),
     ('Grace',  'Gadget G',  8),
     ('Heidi',  'Widget H',  1);"
```

### 5.2 Compare row counts

```bash
echo "=== Source ===" && \
docker exec source-db psql -U postgres -d source -t -c \
  "SELECT count(*) FROM inventory.orders;" && \
echo "=== Sink ===" && \
docker exec sink-db psql -U postgres -d sink -t -c \
  "SELECT count(*) FROM inventory.orders;"
```

Expected: both counts match (allow a second for propagation).

### 5.3 Full diff between source and sink

```bash
docker exec source-db psql -U postgres -d source -t -A -c \
  "SELECT id, customer_name, product, quantity FROM inventory.orders ORDER BY id;" \
  > /tmp/source_rows.txt

docker exec sink-db psql -U postgres -d sink -t -A -c \
  "SELECT id, customer_name, product, quantity FROM inventory.orders ORDER BY id;" \
  > /tmp/sink_rows.txt

diff /tmp/source_rows.txt /tmp/sink_rows.txt && echo "MATCH" || echo "MISMATCH"
```

Expected: `MATCH` (note: `created_at` is excluded because its representation
may differ between source and sink display formats).

---

## 6. Monitoring and health checks

### 6.1 Connector status summary

```bash
curl -s http://localhost:8083/connectors?expand=status | \
  jq '.[] | {name: .status.name, state: .status.connector.state, task_state: .status.tasks[0].state}'
```

Lists all connectors with their state in one call.

### 6.2 Connector task details (useful for debugging failures)

```bash
# Source connector
curl -s http://localhost:8083/connectors/source-debezium-cdc/status | jq '.tasks[]'

# Sink connector
curl -s http://localhost:8083/connectors/sink-jdbc-orders/status | jq '.tasks[]'
```

If a task is `FAILED`, the `trace` field contains the Java stack trace.

### 6.3 Kafka Connect worker info

```bash
curl -s http://localhost:8083/ | jq .
```

Shows Connect version, commit hash, and cluster ID.

### 6.4 List all Kafka topics

```bash
docker exec kcat kcat -b kafka:9092 -L | grep "topic \"" | sed 's/.*topic "\(.*\)".*/\1/' | sort
```

Expected topics include: `poc1.inventory.orders`, `_connect-configs`,
`_connect-offsets`, `_connect-status`.

### 6.5 Topic details (partitions, offsets, replicas)

```bash
docker exec kcat kcat -b kafka:9092 -L -t poc1.inventory.orders
```

Shows partition count, leader, replicas, and ISR.

### 6.6 Consumer group lag

```bash
docker exec kcat kcat -b kafka:9092 -L | grep -A2 "group \"connect-sink-jdbc-orders\""
```

Alternatively, use Kafka UI at [http://localhost:8080](http://localhost:8080)
to see consumer lag visually under the **Consumer Groups** tab.

### 6.7 PostgreSQL replication slot status

```bash
docker exec source-db psql -U postgres -d source -c \
  "SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn
   FROM pg_replication_slots;"
```

Expected: slot `debezium_cdc`, plugin `pgoutput`, `active = t`.

If `active = f`, the source connector is not connected (stopped or failed).

### 6.8 PostgreSQL publication status

```bash
docker exec source-db psql -U postgres -d source -c \
  "SELECT * FROM pg_publication;"
```

Expected: publication `dbz_publication`.

```bash
docker exec source-db psql -U postgres -d source -c \
  "SELECT * FROM pg_publication_tables;"
```

Expected: `inventory.orders` listed under `dbz_publication`. The `inventory.outbox`
table should NOT appear (it is excluded via `table.exclude.list`).

### 6.9 WAL retention / replication slot lag

```bash
docker exec source-db psql -U postgres -d source -c \
  "SELECT slot_name,
          pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
   FROM pg_replication_slots;"
```

Shows how much WAL is being retained by the slot. If this grows large while
the connector is stopped, disk usage will increase.

### 6.10 Kafka Connect logs

```bash
# Last 50 lines
docker logs kafka-connect --tail 50

# Follow live (Ctrl+C to stop)
docker logs kafka-connect -f

# Filter for errors
docker logs kafka-connect 2>&1 | grep -i "ERROR"
```

### 6.11 Event count on topic

```bash
docker exec kcat kcat -b kafka:9092 -t poc1.inventory.orders -C -e -q -c
```

Prints the total number of messages on the topic (including tombstones).

---

## 7. Recovery tests

### 7.1 Restart Kafka Connect — verify no data loss

```bash
# Count rows before
docker exec sink-db psql -U postgres -d sink -t -c "SELECT count(*) FROM inventory.orders;"

# Restart Connect
docker compose restart kafka-connect

# Wait for connectors to resume
sleep 15

# Insert a row while (or after) Connect restarts
docker exec source-db psql -U postgres -d source -c \
  "INSERT INTO inventory.orders (customer_name, product, quantity) VALUES ('Ivan', 'Widget I', 4);"

# Wait for propagation
sleep 5

# Verify Ivan arrived in sink
docker exec sink-db psql -U postgres -d sink -c \
  "SELECT id, customer_name FROM inventory.orders WHERE customer_name = 'Ivan';"
```

Expected: Ivan's row is present. Kafka Connect resumes from the last committed
offset — no data loss.

### 7.2 Restart source database — verify connector recovers

```bash
docker compose restart source-db

# Wait for Postgres to accept connections
sleep 10

# Insert after restart
docker exec source-db psql -U postgres -d source -c \
  "INSERT INTO inventory.orders (customer_name, product, quantity) VALUES ('Judy', 'Gadget J', 6);"

# Wait for propagation
sleep 5

docker exec sink-db psql -U postgres -d sink -c \
  "SELECT id, customer_name FROM inventory.orders WHERE customer_name = 'Judy';"
```

Expected: Judy's row is present. The source connector reconnects automatically
using the replication slot, which retains WAL across restarts.

---

## 8. Cleanup / reset

### 8.1 Delete all test data (keep connectors running)

```bash
docker exec source-db psql -U postgres -d source -c "DELETE FROM inventory.orders;"
```

Wait a few seconds, then verify the sink is also empty:

```bash
docker exec sink-db psql -U postgres -d sink -c "SELECT count(*) FROM inventory.orders;"
```

### 8.2 Re-seed source data

```bash
docker exec source-db psql -U postgres -d source -c \
  "INSERT INTO inventory.orders (customer_name, product, quantity) VALUES
     ('Alice',   'Widget A', 10),
     ('Bob',     'Widget B',  5),
     ('Charlie', 'Gadget C',  2);"
```

### 8.3 Full stack reset (destroy everything)

```bash
docker compose down -v
```

This removes all containers, networks, and volumes. The next `docker compose up -d`
starts from scratch (fresh databases, no connectors registered, no Kafka data).
