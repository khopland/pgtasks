-- Version 3: the dedup index only holds tasks that have a dedup key.
--
-- Most tasks have none, so the index shrinks. More importantly, a full index led by topic_id
-- could serve the per-identifier check in claims, which only needs the identifier index. A
-- queue is often empty when autovacuum analyzes it, right after a burst drained; until the next
-- analyze the planner then expects no rows, every index on topic_id costs the same, and a
-- claim that picked this one read every unfinished task of the topic for each candidate.
--
-- Not compatible with library versions 1 and 2: their push names this index in ON CONFLICT
-- without the predicate, which a partial index doesn't match.

DROP INDEX tasks.queue_dedup_idx;
CREATE UNIQUE INDEX queue_dedup_idx ON tasks.queue (topic_id, dedup_key) WHERE dedup_key IS NOT NULL;

INSERT INTO tasks.meta (singleton, version, compatible_from) VALUES (true, 3, 3)
ON CONFLICT (singleton) DO UPDATE SET version = excluded.version, compatible_from = excluded.compatible_from, updated_at = now();
