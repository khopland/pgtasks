-- Version 2: queue counts per topic, snapshotted by the housekeeping leader, so dashboards and
-- health checks don't count live. Additive, so a library at version 1 still runs against it.

CREATE TABLE tasks.topic_count (
    topic_id     integer PRIMARY KEY REFERENCES tasks.topic (id),
    taken_at     timestamptz NOT NULL,
    scheduled    bigint NOT NULL,
    retrying     bigint NOT NULL,
    ready        bigint NOT NULL,
    active       bigint NOT NULL,
    suspended    bigint NOT NULL,
    failed       bigint NOT NULL,
    due          bigint NOT NULL,
    next_run_at  timestamptz
);

INSERT INTO tasks.meta (singleton, version, compatible_from) VALUES (true, 2, 1)
ON CONFLICT (singleton) DO UPDATE SET version = excluded.version, compatible_from = excluded.compatible_from, updated_at = now();
