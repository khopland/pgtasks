-- Registers a topic. Idempotent: returns the existing topic's id if it is registered already.
-- A new topic is broadcast by the trigger on tasks.topic (R__notify_topic.sql), so every node
-- that has a handler for it starts processing it.
--
-- A topic is a row, not a table, so creating one takes no DDL rights and no lock on tasks.task:
-- the function runs with the caller's rights, which need INSERT and SELECT on tasks.topic. The
-- fixed search_path keeps it from resolving names through the caller's path. Repeatable:
-- re-applied whenever this file changes.

-- The family arguments went with topic families.
DROP FUNCTION IF EXISTS tasks.create_topic(text, text, jsonb);

CREATE OR REPLACE FUNCTION tasks.create_topic(topic_name text)
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
    INSERT INTO tasks.topic (name)
    VALUES (topic_name)
    ON CONFLICT (name) DO NOTHING
    RETURNING id INTO v_id;
    IF v_id IS NULL THEN
        -- Another transaction registered it at the same time.
        SELECT id INTO v_id FROM tasks.topic WHERE name = topic_name;
    END IF;
    RETURN v_id;
END
$$;

REVOKE ALL ON FUNCTION tasks.create_topic(text) FROM PUBLIC;
