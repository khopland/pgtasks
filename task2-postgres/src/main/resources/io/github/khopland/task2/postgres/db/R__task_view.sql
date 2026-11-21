-- Replaces TASK_PRETTY: the current state is the queue state if the task is unfinished,
-- otherwise its outcome. Repeatable. CREATE OR REPLACE VIEW can only add columns at the end;
-- to drop or change one, drop the view in the versioned file that needs it.

CREATE OR REPLACE VIEW tasks.task_view AS
SELECT tp.name AS topic,
       t.id,
       t.identifier,
       coalesce(q.state::text, t.outcome::text) AS state,
       coalesce(q.attempt, t.attempts) AS attempt,
       q.run_at,
       q.priority,
       n.name AS node,
       q.claimed_at,
       t.reference,
       t.parent_id,
       t.root_id,
       t.created_at,
       t.completed_at,
       t.previous_outcome,
       t.payload_type,
       t.payload,
       t.output,
       t.error
FROM tasks.task t
JOIN tasks.topic tp ON tp.id = t.topic_id
LEFT JOIN tasks.queue q ON q.task_id = t.id
LEFT JOIN tasks.node n ON n.id = q.node_id;
