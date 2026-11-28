-- pgtasks schema, version 2: the outcome of a task an operator cancelled. Released files are never
-- edited (docs/operations.md, "Changing the schema"); SchemaFreezeTest pins this one once it ships.
--
-- Nodes of 0.1.0 (schema version 1) still run against it, so compatible_from stays 1: they never
-- write the new value, they read a task's outcome as text, and their housekeeping skips a
-- retention for an outcome it doesn't know, so cancelled tasks wait to be purged until the
-- deploy is done. Adding an enum value rewrites nothing and takes no lock on the tables.

ALTER TYPE ${pgtasks_schema}.task_outcome ADD VALUE IF NOT EXISTS 'cancelled';

INSERT INTO ${pgtasks_schema}.meta (singleton, version, compatible_from) VALUES (true, 2, 1)
ON CONFLICT (singleton) DO UPDATE SET version = excluded.version, compatible_from = excluded.compatible_from, updated_at = now();
