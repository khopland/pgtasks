-- Broadcasts new topics, and topics paused or resumed, so every node starts or
-- stops processing them without waiting for its next heartbeat. The payload is the topic's name;
-- nodes compare all topics with what they run on any notification. Also covers topics registered
-- or paused by hand in SQL. Other changes, such as retention, aren't broadcast. Repeatable.

CREATE OR REPLACE FUNCTION tasks.notify_topic_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('tasks_topic', added.name)
    FROM (SELECT name FROM new_rows) AS added;
    RETURN NULL;
END
$$;

CREATE OR REPLACE FUNCTION tasks.notify_topic_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('tasks_topic', changed.name)
    FROM (SELECT n.name FROM new_rows n JOIN old_rows o ON o.id = n.id
          WHERE n.paused IS DISTINCT FROM o.paused) AS changed;
    RETURN NULL;
END
$$;

CREATE OR REPLACE TRIGGER topic_insert_notify
    AFTER INSERT ON tasks.topic
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION tasks.notify_topic_insert();

CREATE OR REPLACE TRIGGER topic_update_notify
    AFTER UPDATE ON tasks.topic
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION tasks.notify_topic_update();
