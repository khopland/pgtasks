-- revision: 1
-- Broadcasts new and deleted topics, so every node starts or stops processing them without
-- waiting for its next heartbeat. The payload is the topic's name; nodes compare all topics with
-- what they run on any notification. Also covers topics registered or deleted by hand in SQL.
-- Other changes, such as retention, aren't broadcast. Repeatable.

CREATE OR REPLACE FUNCTION ${pgtasks_schema}.notify_topic_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('${pgtasks_schema}_topic', added.name)
    FROM (SELECT name FROM new_rows) AS added;
    RETURN NULL;
END
$$;

CREATE OR REPLACE FUNCTION ${pgtasks_schema}.notify_topic_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('${pgtasks_schema}_topic', removed.name)
    FROM (SELECT name FROM old_rows) AS removed;
    RETURN NULL;
END
$$;

CREATE OR REPLACE TRIGGER topic_delete_notify
    AFTER DELETE ON ${pgtasks_schema}.topic
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT EXECUTE FUNCTION ${pgtasks_schema}.notify_topic_delete();

CREATE OR REPLACE TRIGGER topic_insert_notify
    AFTER INSERT ON ${pgtasks_schema}.topic
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION ${pgtasks_schema}.notify_topic_insert();
