-- Task 2.0 schema, version 1 (plan section 10).
--
-- The small, hot tasks.queue holds unfinished tasks and is the only table polling touches.
-- tasks.task is the record of every task, written once at push and once at completion. It is
-- one table, not partitioned: claims never read it, so history doesn't slow them down, and a
-- plain table can get new indexes with CREATE INDEX CONCURRENTLY and new topics without DDL.
-- Functions, views and triggers live in the repeatable R__ files.
--
-- Plain SQL, applied by the library's runner, Flyway, Liquibase or a DBA pipeline alike.
-- Like every versioned file, this one ends by recording its version in tasks.meta.

CREATE SCHEMA IF NOT EXISTS tasks;

-- scheduled: delayed (push delay or later()); doesn't hold back later tasks under SINGULAR.
-- retrying:  backing off after a failure or throttling; holds its place in line.
-- failed:    dead letter, out of attempts.
CREATE TYPE tasks.queue_state AS ENUM ('scheduled', 'retrying', 'ready', 'active', 'suspended', 'failed');
CREATE TYPE tasks.task_outcome AS ENUM ('succeeded', 'filtered', 'failed', 'redundant', 'recreated');
CREATE TYPE tasks.topic_state AS ENUM ('active', 'sealed', 'archived');

-- One row: the schema version, and the oldest library schema version that can still run
-- against it. Lets old and new nodes share the schema during a rolling deploy.
CREATE TABLE tasks.meta (
    singleton        boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    version          integer NOT NULL CHECK (version > 0),
    compatible_from  integer NOT NULL CHECK (compatible_from BETWEEN 1 AND version),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tasks.topic (
    id          integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        text NOT NULL UNIQUE CHECK (length(name) BETWEEN 1 AND 200),
    family      text,
    variables   jsonb,
    state       tasks.topic_state NOT NULL DEFAULT 'active',
    paused      boolean NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tasks.node (
    id            integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          text NOT NULL,
    version       text,
    started_at    timestamptz NOT NULL DEFAULT now(),
    heartbeat_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX node_heartbeat_idx ON tasks.node (heartbeat_at);

CREATE SEQUENCE tasks.task_id_seq AS bigint;

CREATE TABLE tasks.task (
    id                bigint PRIMARY KEY DEFAULT nextval('tasks.task_id_seq'),
    topic_id          integer NOT NULL REFERENCES tasks.topic (id),
    identifier        text NOT NULL,
    reference         text,
    payload           text,
    payload_type      text,
    parent_id         bigint,
    root_id           bigint,
    trace_context     text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    outcome           tasks.task_outcome,
    previous_outcome  tasks.task_outcome,
    output            text,
    error             jsonb,
    attempts          integer NOT NULL DEFAULT 0,
    completed_at      timestamptz,
    CHECK ((outcome IS NULL) = (completed_at IS NULL))
);
-- History per identifier: REPLACE, DELETE, ETERNAL junctures and recreate.
CREATE INDEX task_identifier_idx ON tasks.task (topic_id, identifier, id);
CREATE INDEX task_reference_idx ON tasks.task (reference) WHERE reference IS NOT NULL;
CREATE INDEX task_root_idx ON tasks.task (root_id) WHERE root_id IS NOT NULL;
-- Resolving a date to a task id; tiny, because created_at grows with the id.
CREATE INDEX task_created_brin ON tasks.task USING brin (created_at);
-- Results by period, and retention: one outcome's expired tasks are one range.
CREATE INDEX task_completed_idx ON tasks.task (topic_id, outcome, completed_at);

CREATE TABLE tasks.queue (
    task_id     bigint PRIMARY KEY REFERENCES tasks.task (id),
    topic_id    integer NOT NULL,
    identifier  text NOT NULL,
    state       tasks.queue_state NOT NULL,
    priority    integer NOT NULL DEFAULT 0,
    run_at      timestamptz,
    attempt     integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    node_id     integer REFERENCES tasks.node (id),
    claimed_at  timestamptz,
    dedup_key   text,
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
CREATE INDEX queue_ready_idx ON tasks.queue (topic_id, priority DESC, task_id) WHERE state = 'ready';
-- Per-identifier ordering, SUPERSEDE/REPLACE/DELETE and junctures.
CREATE INDEX queue_identifier_idx ON tasks.queue (topic_id, identifier, task_id);
-- Promoting due retries and delayed tasks.
CREATE INDEX queue_due_idx ON tasks.queue (run_at) WHERE state IN ('scheduled', 'retrying');
-- Requeueing tasks from dead nodes.
CREATE INDEX queue_active_idx ON tasks.queue (node_id) WHERE state = 'active';
-- Idempotent push: at most one unfinished task per key. Finished tasks leave the queue. Partial,
-- because most tasks have no key, and because a full index led by topic_id would compete with
-- queue_identifier_idx in claims whenever the queue's statistics say it is empty.
CREATE UNIQUE INDEX queue_dedup_idx ON tasks.queue (topic_id, dedup_key) WHERE dedup_key IS NOT NULL;

-- Append-only log of attempts. Records failed and expired attempts.
CREATE TABLE tasks.attempt (
    topic_id     integer NOT NULL,
    task_id      bigint NOT NULL,
    attempt      integer NOT NULL,
    node_id      integer,
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz NOT NULL DEFAULT now(),
    result       text NOT NULL,
    error        jsonb
);
CREATE INDEX attempt_task_idx ON tasks.attempt (task_id, attempt);

-- Runtime overrides of configured limits (plan section 08).
CREATE TABLE tasks.rate_limit (
    name        text PRIMARY KEY,
    rate        double precision CHECK (rate > 0),
    burst       integer CHECK (burst > 0),
    until       timestamptz,
    version     bigint NOT NULL DEFAULT 1,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    updated_by  text
);

-- Strict admission: one token bucket per limit, shared by every node. A claim locks the row,
-- polls at most the available tokens and charges what it got.
CREATE TABLE tasks.rate_admission (
    name         text PRIMARY KEY,
    tokens       double precision NOT NULL CHECK (tokens >= 0),
    refilled_at  timestamptz NOT NULL
);

-- Recurring task definitions (Phase 5).
CREATE TABLE tasks.schedule (
    id           integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic_id     integer NOT NULL REFERENCES tasks.topic (id),
    name         text NOT NULL,
    identifier   text NOT NULL,
    cron         text NOT NULL,
    payload      text,
    next_run_at  timestamptz NOT NULL,
    UNIQUE (topic_id, name)
);

-- Queue counts per topic, snapshotted by the housekeeping leader, so dashboards and health
-- checks don't count live.
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

-- Leases on cluster-wide jobs, such as housekeeping. A node holds a lease by renewing it within
-- its term, so a node that dies or hangs loses it once the term runs out, without closing
-- anything. (A session advisory lock lasts as long as its connection, which a frozen node keeps.)
CREATE TABLE tasks.lease (
    name         text PRIMARY KEY,
    node_id      integer NOT NULL REFERENCES tasks.node (id) ON DELETE CASCADE,
    acquired_at  timestamptz NOT NULL,
    expires_at   timestamptz NOT NULL
);

INSERT INTO tasks.meta (singleton, version, compatible_from) VALUES (true, 1, 1)
ON CONFLICT (singleton) DO UPDATE SET version = excluded.version, compatible_from = excluded.compatible_from, updated_at = now();
