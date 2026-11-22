-- Wake-ups for workers. One statement-level trigger per event instead of a row-level one: a
-- push or promotion of many rows sends one notification per topic. NOTIFY is delivered at
-- commit and dropped on rollback; it is a hint, polling still finds the work. Repeatable.

CREATE OR REPLACE FUNCTION ${pgtasks_schema}.notify_ready()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
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
