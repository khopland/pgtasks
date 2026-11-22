-- Wake-ups for workers. One statement-level trigger per event instead of a row-level one: a
-- push or promotion of many rows sends one notification per topic. NOTIFY is delivered at
-- commit and dropped on rollback; it is a hint, polling still finds the work. Repeatable.
--
-- Postgres serializes the commits of all transactions that notified, behind one lock held until
-- the commit is flushed, so notifying inside a busy application's transactions caps its commit
-- rate near one per flush. A push the engine sees commit (tasks.push(...) in a bound transaction,
-- tasks.independently(), downstream work of a completion) sets ${pgtasks_schema}.notify_after_commit
-- for its own statement: the insert trigger skips it and clears the flag, and the engine notifies
-- after the commit, from a short transaction of its own. Other inserts, such as
-- tasks.on(connection) or plain SQL, still notify here.

CREATE OR REPLACE FUNCTION ${pgtasks_schema}.notify_ready()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' AND current_setting('${pgtasks_schema}.notify_after_commit', true) = 'on' THEN
        PERFORM set_config('${pgtasks_schema}.notify_after_commit', 'off', true);
        RETURN NULL;
    END IF;
    PERFORM pg_notify('${pgtasks_schema}_ready', ready.topic_id::text)
    FROM (SELECT DISTINCT topic_id FROM changed_rows WHERE state = 'ready') AS ready;
    RETURN NULL;
END
$$;

CREATE OR REPLACE TRIGGER queue_insert_notify
    AFTER INSERT ON ${pgtasks_schema}.queue
    REFERENCING NEW TABLE AS changed_rows
    FOR EACH STATEMENT EXECUTE FUNCTION ${pgtasks_schema}.notify_ready();

CREATE OR REPLACE TRIGGER queue_update_notify
    AFTER UPDATE ON ${pgtasks_schema}.queue
    REFERENCING NEW TABLE AS changed_rows
    FOR EACH STATEMENT EXECUTE FUNCTION ${pgtasks_schema}.notify_ready();
