--liquibase formatted sql

-- Task 2.0 schema (plan section 10). The small, hot tasks.queue holds unfinished tasks and is
-- the only table polling touches. tasks.task is the record of every task, partitioned by topic,
-- written once at push and once at completion.

--changeset task2:001-schema
CREATE SCHEMA IF NOT EXISTS ${schema};

--changeset task2:002-types
-- scheduled: delayed (push delay or later()); doesn't hold back later tasks under SINGULAR.
-- retrying:  backing off after a failure or throttling; holds its place in line.
-- failed:    dead letter, out of attempts.
CREATE TYPE ${schema}.queue_state AS ENUM ('scheduled', 'retrying', 'ready', 'active', 'suspended', 'failed');
CREATE TYPE ${schema}.task_outcome AS ENUM ('succeeded', 'filtered', 'failed', 'redundant', 'recreated');
CREATE TYPE ${schema}.topic_state AS ENUM ('active', 'sealed', 'archived');

--changeset task2:003-topic
CREATE TABLE ${schema}.topic (
    id          integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        text NOT NULL UNIQUE CHECK (length(name) BETWEEN 1 AND 200),
    family      text,
    variables   jsonb,
    state       ${schema}.topic_state NOT NULL DEFAULT 'active',
    paused      boolean NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now()
);

--changeset task2:004-node
CREATE TABLE ${schema}.node (
    id            integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          text NOT NULL,
    version       text,
    started_at    timestamptz NOT NULL DEFAULT now(),
    heartbeat_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX node_heartbeat_idx ON ${schema}.node (heartbeat_at);

--changeset task2:005-task
CREATE SEQUENCE ${schema}.task_id_seq AS bigint;
CREATE TABLE ${schema}.task (
    topic_id          integer NOT NULL REFERENCES ${schema}.topic (id),
    id                bigint NOT NULL DEFAULT nextval('${schema}.task_id_seq'),
    identifier        text NOT NULL,
    reference         text,
    payload           text,
    payload_type      text,
    parent_id         bigint,
    root_id           bigint,
    trace_context     text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    outcome           ${schema}.task_outcome,
    previous_outcome  ${schema}.task_outcome,
    output            text,
    error             jsonb,
    attempts          integer NOT NULL DEFAULT 0,
    completed_at      timestamptz,
    PRIMARY KEY (topic_id, id),
    CHECK ((outcome IS NULL) = (completed_at IS NULL))
) PARTITION BY LIST (topic_id);
CREATE INDEX task_identifier_idx ON ${schema}.task (topic_id, identifier, id);
CREATE INDEX task_reference_idx ON ${schema}.task (reference) WHERE reference IS NOT NULL;
CREATE INDEX task_root_idx ON ${schema}.task (root_id) WHERE root_id IS NOT NULL;
CREATE INDEX task_created_brin ON ${schema}.task USING brin (created_at);
CREATE INDEX task_completed_idx ON ${schema}.task (topic_id, completed_at);

--changeset task2:006-queue
CREATE TABLE ${schema}.queue (
    task_id     bigint PRIMARY KEY,
    topic_id    integer NOT NULL,
    identifier  text NOT NULL,
    state       ${schema}.queue_state NOT NULL,
    priority    integer NOT NULL DEFAULT 0,
    run_at      timestamptz,
    attempt     integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    node_id     integer REFERENCES ${schema}.node (id),
    claimed_at  timestamptz,
    dedup_key   text,
    FOREIGN KEY (topic_id, task_id) REFERENCES ${schema}.task (topic_id, id),
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
CREATE INDEX queue_ready_idx ON ${schema}.queue (topic_id, priority DESC, task_id) WHERE state = 'ready';
-- Per-identifier ordering, SUPERSEDE/REPLACE/DELETE and junctures.
CREATE INDEX queue_identifier_idx ON ${schema}.queue (topic_id, identifier, task_id);
-- Promoting due retries and delayed tasks.
CREATE INDEX queue_due_idx ON ${schema}.queue (run_at) WHERE state IN ('scheduled', 'retrying');
-- Requeueing tasks from dead nodes.
CREATE INDEX queue_active_idx ON ${schema}.queue (node_id) WHERE state = 'active';
-- Idempotent push: at most one unfinished task per key. Finished tasks leave the queue.
CREATE UNIQUE INDEX queue_dedup_idx ON ${schema}.queue (topic_id, dedup_key);

--changeset task2:007-attempt
-- Append-only log of attempts. Records failed and expired attempts.
-- Partitioned by month; until housekeeping creates monthly partitions, rows go to the default.
CREATE TABLE ${schema}.attempt (
    topic_id     integer NOT NULL,
    task_id      bigint NOT NULL,
    attempt      integer NOT NULL,
    node_id      integer,
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz NOT NULL DEFAULT now(),
    result       text NOT NULL,
    error        jsonb
) PARTITION BY RANGE (started_at);
CREATE TABLE ${schema}.attempt_default PARTITION OF ${schema}.attempt DEFAULT;
CREATE INDEX attempt_task_idx ON ${schema}.attempt (task_id, attempt);

--changeset task2:008-rate-limit
-- Runtime overrides of configured limits (plan section 08); not used before Phase 3.
CREATE TABLE ${schema}.rate_limit (
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
CREATE TABLE ${schema}.rate_admission (
    name         text PRIMARY KEY,
    tokens       double precision NOT NULL CHECK (tokens >= 0),
    refilled_at  timestamptz NOT NULL
);

--changeset task2:009-schedule
-- Recurring task definitions (Phase 5).
CREATE TABLE ${schema}.schedule (
    id           integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic_id     integer NOT NULL REFERENCES ${schema}.topic (id),
    name         text NOT NULL,
    identifier   text NOT NULL,
    cron         text NOT NULL,
    payload      text,
    next_run_at  timestamptz NOT NULL,
    UNIQUE (topic_id, name)
);

--changeset task2:010-task-view
-- Replaces TASK_PRETTY: the current state is the queue state if the task is unfinished,
-- otherwise its outcome.
CREATE VIEW ${schema}.task_view AS
SELECT tp.name AS topic,
       t.id,
       t.identifier,
       coalesce(q.state::text, t.outcome::text) AS state,
       coalesce(q.attempt, t.attempts) AS attempt,
       q.run_at,
       q.priority,
       n.name AS node,
       q.claimed_at,
       t.reference,
       t.parent_id,
       t.root_id,
       t.created_at,
       t.completed_at,
       t.previous_outcome,
       t.payload_type,
       t.payload,
       t.output,
       t.error
FROM ${schema}.task t
JOIN ${schema}.topic tp ON tp.id = t.topic_id
LEFT JOIN ${schema}.queue q ON q.task_id = t.id
LEFT JOIN ${schema}.node n ON n.id = q.node_id;

--changeset task2:011-create-topic splitStatements:false
-- Registers a topic and creates its partition. SECURITY DEFINER with a fixed search_path, so
-- the application's runtime user needs only EXECUTE, not DDL rights. Idempotent.
CREATE FUNCTION ${schema}.create_topic(topic_name text, topic_family text DEFAULT NULL, topic_variables jsonb DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_id integer;
BEGIN
    SELECT id INTO v_id FROM ${schema}.topic WHERE name = topic_name;
    IF FOUND THEN
        RETURN v_id;
    END IF;
    -- Creating a partition takes a strong lock on tasks.task anyway; serialize creators.
    PERFORM pg_advisory_xact_lock(hashtext('${schema}.create_topic'));
    SELECT id INTO v_id FROM ${schema}.topic WHERE name = topic_name;
    IF FOUND THEN
        RETURN v_id;
    END IF;
    INSERT INTO ${schema}.topic (name, family, variables)
    VALUES (topic_name, topic_family, topic_variables)
    RETURNING id INTO v_id;
    EXECUTE format('CREATE TABLE %I.%I PARTITION OF %I.task FOR VALUES IN (%s)', '${schema}', 'task_' || v_id, '${schema}', v_id);
    PERFORM pg_notify('${schema}_topic', topic_name);
    RETURN v_id;
END
$$;

--changeset task2:012-create-topic-grants
REVOKE ALL ON FUNCTION ${schema}.create_topic(text, text, jsonb) FROM PUBLIC;

--changeset task2:013-notify splitStatements:false
-- One statement-level trigger per event instead of a row-level one: a push or promotion of
-- many rows sends one notification per topic. NOTIFY is delivered at commit and dropped on
-- rollback; it is a hint, polling still finds the work.
CREATE FUNCTION ${schema}.notify_ready()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('${schema}_ready', ready.topic_id::text)
    FROM (SELECT DISTINCT topic_id FROM changed_rows WHERE state = 'ready') AS ready;
    RETURN NULL;
END
$$;

--changeset task2:014-notify-triggers
CREATE TRIGGER queue_insert_notify
    AFTER INSERT ON ${schema}.queue
    REFERENCING NEW TABLE AS changed_rows
    FOR EACH STATEMENT EXECUTE FUNCTION ${schema}.notify_ready();
CREATE TRIGGER queue_update_notify
    AFTER UPDATE ON ${schema}.queue
    REFERENCING NEW TABLE AS changed_rows
    FOR EACH STATEMENT EXECUTE FUNCTION ${schema}.notify_ready();
