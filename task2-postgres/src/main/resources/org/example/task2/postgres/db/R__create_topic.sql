-- Registers a topic and creates its partition of tasks.task. Idempotent.
--
-- SECURITY DEFINER with a fixed search_path: the function runs as the schema owner, so the
-- application's runtime user needs only EXECUTE on it, not DDL rights. Repeatable: re-applied
-- whenever this file changes.

CREATE OR REPLACE FUNCTION tasks.create_topic(topic_name text, topic_family text DEFAULT NULL, topic_variables jsonb DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_id integer;
BEGIN
    SELECT id INTO v_id FROM tasks.topic WHERE name = topic_name;
    IF FOUND THEN
        RETURN v_id;
    END IF;
    -- Creating a partition takes a strong lock on tasks.task anyway; serialize creators.
    PERFORM pg_advisory_xact_lock(hashtext('tasks.create_topic'));
    SELECT id INTO v_id FROM tasks.topic WHERE name = topic_name;
    IF FOUND THEN
        RETURN v_id;
    END IF;
    INSERT INTO tasks.topic (name, family, variables)
    VALUES (topic_name, topic_family, topic_variables)
    RETURNING id INTO v_id;
    EXECUTE format('CREATE TABLE tasks.%I PARTITION OF tasks.task FOR VALUES IN (%s)', 'task_' || v_id, v_id);
    PERFORM pg_notify('tasks_topic', topic_name);
    RETURN v_id;
END
$$;

REVOKE ALL ON FUNCTION tasks.create_topic(text, text, jsonb) FROM PUBLIC;
