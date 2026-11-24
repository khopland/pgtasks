-- pgtasks schema, version 1. Not released yet, so it may still change in place. Once released,
-- never edit it: add V002 instead (docs/operations.md, "Changing the schema"), and pin its
-- checksum in SchemaFreezeTest.
--
-- The small, hot queue table holds unfinished tasks and is the only table polling touches.
-- The task table is the record of every task, written once at push and once at completion. It is
-- one table, not partitioned: claims never read it, so history doesn't slow them down, and a
-- plain table can get new indexes with CREATE INDEX CONCURRENTLY and new topics without DDL.
-- Functions, views and triggers live in the repeatable R__ files.
--
-- Plain SQL, applied by the library's runner, Flyway, Liquibase or a DBA pipeline alike. Every
-- name is qualified with the placeholder pgtasks_schema, the schema's name (pgtasks by default):
-- the runner fills it in, Flyway takes it as a placeholder, Liquibase as a changelog parameter.
-- Like every versioned file, this one ends by recording its version in the meta table.

CREATE SCHEMA IF NOT EXISTS ${pgtasks_schema};

-- scheduled: delayed (push delay or later()); doesn't hold back later tasks under SINGULAR.
-- retrying:  backing off after a failure; holds its place in line.
-- failed:    dead letter, out of attempts.
CREATE TYPE ${pgtasks_schema}.queue_state AS ENUM ('scheduled', 'retrying', 'ready', 'active', 'suspended', 'failed');
CREATE TYPE ${pgtasks_schema}.task_outcome AS ENUM ('succeeded', 'filtered', 'failed', 'redundant', 'recreated');

-- One row: the schema version, and the oldest library schema version that can still run
-- against it. Lets old and new nodes share the schema during a rolling deploy.
CREATE TABLE ${pgtasks_schema}.meta (
    singleton        boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    version          integer NOT NULL CHECK (version > 0),
    compatible_from  integer NOT NULL CHECK (compatible_from BETWEEN 1 AND version),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ${pgtasks_schema}.topic (
    id          integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        text NOT NULL UNIQUE CHECK (length(name) BETWEEN 1 AND 200),
    -- How long finished tasks are kept per outcome, as ISO periods: {"succeeded": "P30D"}. Set by
    -- the nodes that run the topic, so whichever node leads housekeeping applies it. NULL: kept forever.
    retention   jsonb CHECK (jsonb_typeof(retention) = 'object'),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ${pgtasks_schema}.node (
    id            integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          text NOT NULL,
    version       text,
    started_at    timestamptz NOT NULL DEFAULT now(),
    heartbeat_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX node_heartbeat_idx ON ${pgtasks_schema}.node (heartbeat_at);

CREATE SEQUENCE ${pgtasks_schema}.task_id_seq AS bigint;

CREATE TABLE ${pgtasks_schema}.task (
    id                bigint PRIMARY KEY DEFAULT nextval('${pgtasks_schema}.task_id_seq'),
    topic_id          integer NOT NULL REFERENCES ${pgtasks_schema}.topic (id),
    identifier        text NOT NULL,
    reference         text,
    payload           text,
    payload_type      text,
    parent_id         bigint,
    root_id           bigint,
    -- Names and values pushed with the task: the creation's own and the observers', such as
    -- trace propagation headers or MDC values. NULL when there are none.
    headers           jsonb,
    created_at        timestamptz NOT NULL DEFAULT now(),
    outcome           ${pgtasks_schema}.task_outcome,
    previous_outcome  ${pgtasks_schema}.task_outcome,
    output            text,
    error             jsonb,
    attempts          integer NOT NULL DEFAULT 0,
    completed_at      timestamptz,
    CHECK ((outcome IS NULL) = (completed_at IS NULL))
) WITH (
    -- Every finished task leaves dead rows here: the completion update (never HOT, since outcome
    -- and completed_at are indexed) and, later, the retention delete. With the default 20 %,
    -- 25 million rows of history wait for 5 million dead ones between vacuums, and the heap and
    -- indexes bloat meanwhile, as the sustained-load benchmark showed. 1 % vacuums every 250,000.
    autovacuum_vacuum_scale_factor = 0.01
);
-- History per identifier: REPLACE, and recreate.
CREATE INDEX task_identifier_idx ON ${pgtasks_schema}.task (topic_id, identifier, id);
CREATE INDEX task_reference_idx ON ${pgtasks_schema}.task (reference) WHERE reference IS NOT NULL;
CREATE INDEX task_root_idx ON ${pgtasks_schema}.task (root_id) WHERE root_id IS NOT NULL;
-- Resolving a date to a task id; tiny, because created_at grows with the id.
CREATE INDEX task_created_brin ON ${pgtasks_schema}.task USING brin (created_at);
-- Results by period, and retention: one outcome's expired tasks are one range.
CREATE INDEX task_completed_idx ON ${pgtasks_schema}.task (topic_id, outcome, completed_at);

CREATE TABLE ${pgtasks_schema}.queue (
    task_id     bigint PRIMARY KEY REFERENCES ${pgtasks_schema}.task (id),
    topic_id    integer NOT NULL,
    identifier  text NOT NULL,
    state       ${pgtasks_schema}.queue_state NOT NULL,
    priority    integer NOT NULL DEFAULT 0,
    run_at      timestamptz,
    attempt     integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    node_id     integer REFERENCES ${pgtasks_schema}.node (id),
    claimed_at  timestamptz,
    dedup_key   text,
    -- Failed runs in a row, counted or not: the backoff grows with them, so an outage that
    -- doesn't use up attempts still backs off. first_failed_at is when the first of them failed.
    failures         integer NOT NULL DEFAULT 0,
    first_failed_at  timestamptz,
    -- Times the task's node died under it: requeued with a backoff, dead-lettered at the third.
    expiries         integer NOT NULL DEFAULT 0,
    CHECK ((state = 'active') = (node_id IS NOT NULL)),
    CHECK ((state IN ('scheduled', 'retrying')) = (run_at IS NOT NULL))
) WITH (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 500,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_analyze_threshold = 500
);
-- Polling: holds only ready rows, so it stays tiny.
CREATE INDEX queue_ready_idx ON ${pgtasks_schema}.queue (topic_id, priority DESC, task_id) WHERE state = 'ready';
-- Per-identifier ordering, SUPERSEDE and REPLACE.
CREATE INDEX queue_identifier_idx ON ${pgtasks_schema}.queue (topic_id, identifier, task_id);
-- Promoting due retries and delayed tasks.
CREATE INDEX queue_due_idx ON ${pgtasks_schema}.queue (run_at) WHERE state IN ('scheduled', 'retrying');
-- Requeueing tasks from dead nodes.
CREATE INDEX queue_active_idx ON ${pgtasks_schema}.queue (node_id) WHERE state = 'active';
-- How many tasks are failing, and since when, for the count snapshots: few next to the backlog.
CREATE INDEX queue_failing_idx ON ${pgtasks_schema}.queue (topic_id, first_failed_at) WHERE failures > 0;
-- Listing, reviving and discarding a topic's dead letters, which are few next to its backlog.
CREATE INDEX queue_failed_idx ON ${pgtasks_schema}.queue (topic_id, task_id) WHERE state = 'failed';
-- Idempotent push: at most one unfinished task per key. Finished tasks leave the queue. Partial,
-- because most tasks have no key, and because a full index led by topic_id would compete with
-- queue_identifier_idx in claims whenever the queue's statistics say it is empty.
CREATE UNIQUE INDEX queue_dedup_idx ON ${pgtasks_schema}.queue (topic_id, dedup_key) WHERE dedup_key IS NOT NULL;

-- Queue counts per topic, snapshotted by the housekeeping leader, so dashboards and health
-- checks don't count live.
CREATE TABLE ${pgtasks_schema}.topic_count (
    topic_id         integer PRIMARY KEY REFERENCES ${pgtasks_schema}.topic (id),
    taken_at         timestamptz NOT NULL,
    scheduled        bigint NOT NULL,
    retrying         bigint NOT NULL,
    ready            bigint NOT NULL,
    active           bigint NOT NULL,
    suspended        bigint NOT NULL,
    failed           bigint NOT NULL,
    due              bigint NOT NULL,
    next_run_at      timestamptz,
    -- When the oldest ready task was pushed: how long the topic's work has been waiting.
    oldest_ready_at  timestamptz,
    -- Unfinished tasks whose last run failed, and when the longest-failing one first failed.
    failing            bigint NOT NULL DEFAULT 0,
    oldest_failing_at  timestamptz
);

-- Leases on cluster-wide jobs, such as housekeeping. A node holds a lease by renewing it within
-- its term, so a node that dies or hangs loses it once the term runs out, without closing
-- anything. (A session advisory lock lasts as long as its connection, which a frozen node keeps.)
CREATE TABLE ${pgtasks_schema}.lease (
    name         text PRIMARY KEY,
    node_id      integer NOT NULL REFERENCES ${pgtasks_schema}.node (id) ON DELETE CASCADE,
    acquired_at  timestamptz NOT NULL,
    expires_at   timestamptz NOT NULL
);

INSERT INTO ${pgtasks_schema}.meta (singleton, version, compatible_from) VALUES (true, 1, 1)
ON CONFLICT (singleton) DO UPDATE SET version = excluded.version, compatible_from = excluded.compatible_from, updated_at = now();
