-- Registers a topic. Idempotent: returns the existing topic's id if it is registered already.
-- Sends NOTIFY tasks_topic when it registers a new one, so every node can start processing it.
--
-- A topic is a row, not a table, so creating one takes no DDL rights and no lock on tasks.task:
-- the function runs with the caller's rights, which need INSERT and SELECT on tasks.topic. The
-- fixed search_path keeps it from resolving names through the caller's path. Repeatable:
-- re-applied whenever this file changes.

CREATE OR REPLACE FUNCTION tasks.create_topic(topic_name text, topic_family text DEFAULT NULL, topic_variables jsonb DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_id integer;
BEGIN
    -- Look first, so that registering existing topics at every start uses no identity values.
    SELECT id INTO v_id FROM tasks.topic WHERE name = topic_name;
    IF FOUND THEN
        RETURN v_id;
    END IF;
    INSERT INTO tasks.topic (name, family, variables)
    VALUES (topic_name, topic_family, topic_variables)
    ON CONFLICT (name) DO NOTHING
    RETURNING id INTO v_id;
    IF v_id IS NULL THEN
        -- Another transaction registered it at the same time.
        SELECT id INTO v_id FROM tasks.topic WHERE name = topic_name;
    ELSE
        PERFORM pg_notify('tasks_topic', topic_name);
    END IF;
    RETURN v_id;
END
$$;

REVOKE ALL ON FUNCTION tasks.create_topic(text, text, jsonb) FROM PUBLIC;
