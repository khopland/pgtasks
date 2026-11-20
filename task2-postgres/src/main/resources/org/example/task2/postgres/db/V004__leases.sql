-- Version 4: leases on cluster-wide jobs, and an index that retention can use.
--
-- A node holds a lease by renewing it within its term, so a node that dies or hangs loses it
-- once the term runs out, without closing anything. A session advisory lock would last as long
-- as its connection, which a frozen node keeps open while its jobs stand still.
--
-- Retention deletes one outcome at a time, oldest first. Led by (topic_id, completed_at), the
-- index made it read every finished task of other outcomes that are kept longer, on every run;
-- with outcome second, each outcome's expired tasks are one range.

CREATE TABLE tasks.lease (
    name         text PRIMARY KEY,
    node_id      integer NOT NULL REFERENCES tasks.node (id) ON DELETE CASCADE,
    acquired_at  timestamptz NOT NULL,
    expires_at   timestamptz NOT NULL
);

DROP INDEX tasks.task_completed_idx;
CREATE INDEX task_completed_idx ON tasks.task (topic_id, outcome, completed_at);

INSERT INTO tasks.meta (singleton, version, compatible_from) VALUES (true, 4, 3)
ON CONFLICT (singleton) DO UPDATE SET version = excluded.version, compatible_from = excluded.compatible_from, updated_at = now();
