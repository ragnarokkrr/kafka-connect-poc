-- Source database initialization
-- Creates the inventory schema and seed data for CDC testing.

CREATE SCHEMA IF NOT EXISTS inventory;

CREATE TABLE inventory.orders (
    id          serial       PRIMARY KEY,
    customer_name text       NOT NULL,
    product     text         NOT NULL,
    quantity    int          NOT NULL,
    created_at  timestamptz  NOT NULL DEFAULT now()
);

-- Full replica identity so Debezium populates the "before" image on updates
-- and deletes with all column values (not just the PK).
ALTER TABLE inventory.orders REPLICA IDENTITY FULL;

-- Seed rows so CDC has something to capture immediately after connector starts
INSERT INTO inventory.orders (customer_name, product, quantity) VALUES
    ('Alice',   'Widget A', 10),
    ('Bob',     'Widget B',  5),
    ('Charlie', 'Gadget C',  2);

-- Outbox table for the transactional outbox pattern.
-- Applications insert domain events here in the same transaction as business
-- data changes. Debezium captures these inserts and the EventRouter SMT
-- routes each event to a Kafka topic based on the aggregatetype column.
CREATE TABLE inventory.outbox (
    id            uuid         NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregatetype varchar(255) NOT NULL,
    aggregateid   varchar(255) NOT NULL,
    type          varchar(255) NOT NULL,
    payload       jsonb,
    created_at    timestamptz  NOT NULL DEFAULT now()
);

ALTER TABLE inventory.outbox REPLICA IDENTITY FULL;
