-- pgtasks schema, version 2: the outcome of a task an operator cancelled before it ran
-- (TaskAdmin.cancel). Once released, never edit this file; add V003 instead.
--
-- Version 1 nodes keep running against it during a rolling deploy, so compatible_from stays 1:
-- they never write the new value, read outcomes as text, skip retention set for an outcome they
-- don't know, and only see 'cancelled' where they show a task's state. ADD VALUE may run in a
-- transaction (Postgres 12 and later) as long as nothing in it uses the value, and nothing here does.

ALTER TYPE ${pgtasks_schema}.task_outcome ADD VALUE IF NOT EXISTS 'cancelled';

INSERT INTO ${pgtasks_schema}.meta (singleton, version, compatible_from) VALUES (true, 2, 1)
ON CONFLICT (singleton) DO UPDATE SET version = excluded.version, compatible_from = excluded.compatible_from, updated_at = now();
