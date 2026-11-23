CREATE TABLE orders (
    id           text PRIMARY KEY,
    request_id   text NOT NULL UNIQUE,
    customer_id  text NOT NULL,
    amount_cents bigint NOT NULL,
    status       text NOT NULL,
    payment_ref  text,
    reserved_at  timestamptz,
    paid_at      timestamptz,
    shipped_at   timestamptz,
    tracking     text,
    notified_at  timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX orders_status_idx ON orders (status, created_at);

CREATE TABLE order_line (
    order_id text NOT NULL REFERENCES orders (id),
    sku      text NOT NULL,
    quantity int  NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (order_id, sku)
);

CREATE TABLE stock (
    sku       text PRIMARY KEY,
    on_hand   int NOT NULL,
    reserved  int NOT NULL DEFAULT 0,
    CHECK (reserved >= 0)
);
INSERT INTO stock (sku, on_hand) SELECT 'sku-' || n, 1000000 FROM generate_series(1, 50) n;

-- One row per order and kind: a reservation is taken and released at most once.
CREATE TABLE stock_movement (
    order_id   text NOT NULL,
    kind       text NOT NULL CHECK (kind IN ('reserve', 'release')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, kind)
);

-- Notifications the service believes it sent, per order and kind.
CREATE TABLE notification_log (
    order_id   text NOT NULL,
    kind       text NOT NULL,
    sent_count int  NOT NULL DEFAULT 1,
    PRIMARY KEY (order_id, kind)
);

CREATE TABLE reconcile_run (
    id          bigserial PRIMARY KEY,
    node        text NOT NULL,
    ran_at      timestamptz NOT NULL DEFAULT now(),
    stuck       int NOT NULL
);

-- The pgtasks schema is migrated before this file by its own Flyway instance (PgtasksFlyway).
GRANT USAGE ON SCHEMA pgtasks TO orders_app;
GRANT EXECUTE ON FUNCTION pgtasks.create_topic(text) TO orders_app;
