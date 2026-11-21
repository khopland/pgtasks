-- Broadcasts changes to rate limit overrides, so every node applies them without waiting for its
-- next heartbeat. The payload is the limit's name; nodes reload the overrides on any notification.
-- Also covers an operator changing tasks.rate_limit by hand. Repeatable.

CREATE OR REPLACE FUNCTION tasks.notify_limit()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('tasks_limit', changed.name)
    FROM (SELECT DISTINCT name FROM changed_rows) AS changed;
    RETURN NULL;
END
$$;

CREATE OR REPLACE TRIGGER rate_limit_insert_notify
    AFTER INSERT ON tasks.rate_limit
    REFERENCING NEW TABLE AS changed_rows
    FOR EACH STATEMENT EXECUTE FUNCTION tasks.notify_limit();

CREATE OR REPLACE TRIGGER rate_limit_update_notify
    AFTER UPDATE ON tasks.rate_limit
    REFERENCING NEW TABLE AS changed_rows
    FOR EACH STATEMENT EXECUTE FUNCTION tasks.notify_limit();

CREATE OR REPLACE TRIGGER rate_limit_delete_notify
    AFTER DELETE ON tasks.rate_limit
    REFERENCING OLD TABLE AS changed_rows
    FOR EACH STATEMENT EXECUTE FUNCTION tasks.notify_limit();
