-- Sink database initialization
-- Pre-creates the target table to match the source schema. The JDBC sink
-- could auto-create it via schema.evolution=basic (schemas are embedded),
-- but pre-creating keeps the schema explicit and version-controlled.

CREATE SCHEMA IF NOT EXISTS inventory;

CREATE TABLE inventory.orders (
    id          int          PRIMARY KEY,
    customer_name text       NOT NULL,
    product     text         NOT NULL,
    quantity    int          NOT NULL,
    created_at  timestamptz  NOT NULL
);
