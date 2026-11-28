-- One row per paused topic, shared by every node; a resume deletes it. Keyed by name like the
-- extension's other state, and the housekeeping leader deletes the rows of deleted topics.
CREATE TABLE ${pgtasks_schema}.topic_pause (
    topic           text PRIMARY KEY,
    paused_at       timestamptz NOT NULL
);
