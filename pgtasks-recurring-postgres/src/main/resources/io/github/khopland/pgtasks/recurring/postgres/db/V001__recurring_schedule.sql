-- Recurring tasks' schedules, shared by every node: when each runs next. A row is keyed by topic
-- name and schedule name and holds no payload, since a node fires only the schedules it declares.
-- A due row is locked while its run is pushed, and moved on in the same transaction, so a run
-- fires once. seen_at is when a node last declared it; rows unseen for 30 days are deleted.
CREATE TABLE ${pgtasks_schema}.recurring_schedule (
    topic        text NOT NULL,
    name         text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    cron         text NOT NULL,
    zone         text NOT NULL,
    next_run_at  timestamptz NOT NULL,
    last_run_at  timestamptz,
    seen_at      timestamptz NOT NULL,
    PRIMARY KEY (topic, name)
);
