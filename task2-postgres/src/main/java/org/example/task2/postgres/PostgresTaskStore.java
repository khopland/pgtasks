package org.example.task2.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import javax.sql.DataSource;

import org.example.task2.Insertion;
import org.example.task2.Juncture;
import org.example.task2.Ordering;
import org.example.task2.Pushed;
import org.example.task2.spi.StaleClaimException;
import org.example.task2.spi.StoreTransaction;
import org.example.task2.spi.TaskStore;

/**
 * The task store on the {@code tasks} schema (see {@link TaskSchema}). Every operation is one
 * statement, except a claim: it first pins its plan with transaction-local planner settings, and
 * under a strict rate limit also reads and charges the limit's token bucket in the same
 * transaction. When an engine starts, the store migrates or
 * validates the schema according to its {@link SchemaMode}.
 */
public final class PostgresTaskStore implements TaskStore {

    private final DataSource dataSource;
    private final SchemaMode mode;
    private final NamedSql createTopic, findTopic, listTopics, pauseTopic, registerNode, heartbeat, ensureAdmission, admissionLock, admissionCharge;
    private final NamedSql finish, wait, promote, expire, findTask, countQueue, listAttempts, activeClaims, purge, snapshot, snapshots, claimPlanning;
    private final NamedSql deadLetters, revive, discard, notifyReady;
    private final NamedSql lead, resign, setRetention, retention, limitOverrides, overrideLimit, limitConfigured;
    private final Map<Juncture, NamedSql> junctions = new EnumMap<>(Juncture.class);
    private final Map<Insertion, NamedSql> pushes = new EnumMap<>(Insertion.class);
    private final Map<Ordering, NamedSql> polls = new EnumMap<>(Ordering.class);

    /** A store that migrates the schema with the built-in runner when an engine starts. */
    public PostgresTaskStore(DataSource dataSource) {
        this(dataSource, SchemaMode.MIGRATE);
    }

    public PostgresTaskStore(DataSource dataSource, SchemaMode mode) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.mode = Objects.requireNonNull(mode, "mode");
        // The queue's statistics are often those of an empty table: autovacuum tends to analyze
        // it right after a burst drained, and until the next analyze the planner expects no rows
        // at all. Every plan then looks free, and the cheapest-looking one can compare each queued
        // task with each task of a batch. So statements on the queue leave the planner no such
        // choice: rows a claim, promotion or expiry selected and locked are changed by ctid (a
        // TID scan; nothing else can move them while the lock is held, and a row that a
        // concurrent commit moved is skipped, which only delays it), other rows by
        // "task_id = ANY (array)" (pushes keep task_id so a superseded row moved by a concurrent
        // commit is still found), the per-identifier check in claims is fenced (pollSql), and a
        // claim pins its plan (claimPlanning). ClaimPlanTest drains a burst on a queue analyzed
        // while empty.
        createTopic = sql("SELECT tasks.create_topic(:name)");
        findTopic = sql("SELECT id FROM tasks.topic WHERE name = :name");
        listTopics = sql("SELECT id, name, paused FROM tasks.topic ORDER BY id");
        // The topic triggers notify every node when paused actually changes (R__notify_topic.sql).
        pauseTopic = sql("UPDATE tasks.topic SET paused = :paused::boolean WHERE name = :name RETURNING id");
        registerNode = sql("INSERT INTO tasks.node (name, version) VALUES (:name, :version) RETURNING id");
        heartbeat = sql("UPDATE tasks.node SET heartbeat_at = now() WHERE id = :node");
        ensureAdmission = sql("""
            INSERT INTO tasks.rate_admission (name, tokens, refilled_at)
            VALUES (:name, :burst, clock_timestamp())
            ON CONFLICT (name) DO NOTHING
            """);
        // Locks the bucket, so claims under one limit are serialized across nodes. The estimate
        // may use a clock reading from before the lock was granted; that only underestimates.
        admissionLock = sql("""
            SELECT least(:burst::double precision,
                         tokens + :rate::double precision * greatest(0, extract(epoch FROM clock_timestamp() - refilled_at)::double precision))
            FROM tasks.rate_admission
            WHERE name = :name
            FOR UPDATE
            """);
        // Runs after the poll while the lock is held, so the refill is exact; refilled_at never moves back.
        admissionCharge = sql("""
            UPDATE tasks.rate_admission
            SET tokens = greatest(0, least(:burst::double precision,
                                           tokens + :rate::double precision * greatest(0, extract(epoch FROM clock_timestamp() - refilled_at)::double precision))
                                     - :claimed::integer),
                refilled_at = greatest(refilled_at, clock_timestamp())
            WHERE name = :name
            """);
        for (Insertion insertion : Insertion.values()) {
            pushes.put(insertion, sql(pushSql(insertion)));
        }
        for (Ordering ordering : Ordering.values()) {
            polls.put(ordering, sql(pollSql(ordering)).withCustomPlans());
        }
        // A claim has one right plan: walk queue_ready_idx in order, look up each candidate's
        // identifier, update the picked rows by ctid, read their payloads by primary key. While
        // the queue's statistics say it is empty, the planner can't tell that plan from a bitmap
        // scan of the whole topic followed by a sort, which looks up every ready task's
        // identifier on every claim (the Phase 2 benchmark: 4 times slower at 20,000 tasks, and
        // quadratic in the size of a burst). So the claim's own transaction rules the others out.
        claimPlanning = sql("""
            SELECT set_config('enable_seqscan', 'off', true), set_config('enable_bitmapscan', 'off', true),
                   set_config('enable_sort', 'off', true), set_config('enable_hashjoin', 'off', true),
                   set_config('enable_mergejoin', 'off', true)
            """);
        // Fenced: only rows still active under this node with the claimed attempt match.
        finish = sql("""
            WITH d AS (
                SELECT * FROM unnest(:ids::bigint[], :attempts::integer[], :outcomes::text[], :outputs::text[])
                    AS d(task_id, attempt, outcome, output)
            ),
            done AS (
                DELETE FROM tasks.queue q
                USING d
                WHERE q.task_id = ANY (:ids::bigint[])
                  AND q.task_id = d.task_id
                  AND q.attempt = d.attempt
                  AND q.node_id = :node::integer
                  AND q.topic_id = :topic::integer
                  AND q.state = 'active'
                RETURNING q.task_id, q.attempt, d.outcome, d.output
            )
            UPDATE tasks.task t
            SET outcome = done.outcome::tasks.task_outcome,
                output = done.output,
                attempts = done.attempt,
                completed_at = now()
            FROM done
            WHERE t.topic_id = :topic::integer AND t.id = done.task_id
            RETURNING t.id
            """);
        // Non-counted waits (suspended, later, throttled) give the attempt back. That is safe for
        // fencing: only the current owner can do it, so a stale claim's attempt never comes back.
        wait = sql("""
            WITH d AS (
                SELECT * FROM unnest(:ids::bigint[], :attempts::integer[], :states::text[], :delays::bigint[],
                                     :uncounted::integer[], :errors::text[], :started::timestamptz[])
                    AS d(task_id, attempt, state, delay_ms, uncounted, error, started_at)
            ),
            moved AS (
                UPDATE tasks.queue q
                SET state = d.state::tasks.queue_state,
                    run_at = CASE WHEN d.delay_ms IS NULL THEN NULL ELSE now() + d.delay_ms * interval '1 millisecond' END,
                    node_id = NULL,
                    claimed_at = NULL,
                    attempt = q.attempt - d.uncounted
                FROM d
                WHERE q.task_id = ANY (:ids::bigint[])
                  AND q.task_id = d.task_id
                  AND q.attempt = d.attempt
                  AND q.node_id = :node::integer
                  AND q.topic_id = :topic::integer
                  AND q.state = 'active'
                RETURNING q.task_id, q.state, q.attempt
            ),
            dead AS (
                UPDATE tasks.task t
                SET error = d.error::jsonb, attempts = m.attempt
                FROM moved m JOIN d ON d.task_id = m.task_id
                WHERE t.topic_id = :topic::integer AND t.id = m.task_id AND m.state = 'failed'
            ),
            logged AS (
                INSERT INTO tasks.attempt (topic_id, task_id, attempt, node_id, started_at, result, error)
                SELECT :topic::integer, d.task_id, d.attempt, :node::integer, coalesce(d.started_at, now()), 'failed', d.error::jsonb
                FROM moved m JOIN d ON d.task_id = m.task_id
                WHERE d.error IS NOT NULL
            )
            SELECT task_id FROM moved
            """);
        // A LIMIT ... FOR UPDATE SKIP LOCKED selection must be materialized: as the inner side of a
        // semi-join it can be rescanned, and each rescan skips the rows this statement already
        // changed and returns the next ones, so the limit stops holding. The time is read once in
        // a subquery: compared with clock_timestamp() directly, which is volatile, run_at can't
        // bound the index scan, and every promotion walked past every task not due yet (found by
        // the drained-queue pass of the TCK).
        promote = sql("""
            WITH due AS MATERIALIZED (
                SELECT o.ctid AS row FROM tasks.queue o
                WHERE o.state IN ('scheduled', 'retrying') AND o.run_at <= (SELECT clock_timestamp())
                ORDER BY o.run_at
                LIMIT :limit::integer
                FOR UPDATE OF o SKIP LOCKED
            )
            UPDATE tasks.queue q
            SET state = 'ready', run_at = NULL
            WHERE q.ctid = ANY (ARRAY(SELECT row FROM due))
            RETURNING q.topic_id
            """);
        // The attempt is kept: an expired run counts, since a poison task can take a node down.
        expire = sql("""
            WITH stale AS (
                SELECT o.ctid AS row, o.task_id, o.topic_id, o.attempt, o.node_id, o.claimed_at
                FROM tasks.queue o
                WHERE o.state = 'active'
                  AND o.node_id <> :node::integer
                  AND o.node_id IN (SELECT id FROM tasks.node WHERE heartbeat_at < now() - :expiry::bigint * interval '1 millisecond')
                FOR UPDATE OF o SKIP LOCKED
            ),
            requeued AS (
                UPDATE tasks.queue q
                SET state = 'ready', node_id = NULL, claimed_at = NULL
                WHERE q.ctid = ANY (ARRAY(SELECT row FROM stale))
                RETURNING q.task_id, q.topic_id
            ),
            logged AS (
                INSERT INTO tasks.attempt (topic_id, task_id, attempt, node_id, started_at, result)
                SELECT s.topic_id, s.task_id, s.attempt, s.node_id, coalesce(s.claimed_at, now()), 'expired'
                FROM stale s JOIN requeued r ON r.task_id = s.task_id
            )
            SELECT topic_id FROM requeued
            """);
        for (Juncture juncture : Juncture.values()) {
            junctions.put(juncture, sql(junctionSql(juncture)));
        }
        findTask = sql("""
            SELECT tp.name AS topic, t.id, t.identifier, coalesce(q.state::text, t.outcome::text) AS state, q.task_id IS NULL AS finished,
                   coalesce(q.attempt, t.attempts) AS attempt, q.run_at, coalesce(q.priority, 0) AS priority, q.node_id,
                   t.reference, t.parent_id, t.root_id, t.payload, t.payload_type, t.output, t.error::text AS error,
                   t.previous_outcome::text AS previous_outcome, t.created_at, t.completed_at
            FROM tasks.task t
            JOIN tasks.topic tp ON tp.id = t.topic_id
            LEFT JOIN tasks.queue q ON q.task_id = t.id
            WHERE t.id = :id::bigint
            """);
        // Dead letters are few, so queue_failed_idx keeps these to the topic's dead letters.
        deadLetters = sql("""
            SELECT tp.name AS topic, t.id, t.identifier, q.state::text AS state, false AS finished,
                   q.attempt, q.run_at, q.priority, q.node_id,
                   t.reference, t.parent_id, t.root_id, t.payload, t.payload_type, t.output, t.error::text AS error,
                   t.previous_outcome::text AS previous_outcome, t.created_at, t.completed_at
            FROM tasks.queue q
            JOIN tasks.task t ON t.id = q.task_id
            JOIN tasks.topic tp ON tp.id = q.topic_id
            WHERE q.topic_id = :topic::integer AND q.state = 'failed' AND q.task_id > :after::bigint
            ORDER BY q.task_id
            LIMIT :limit::integer
            """);
        String pickDeadLetters = """
            WITH picked AS MATERIALIZED (
                SELECT o.ctid AS row FROM tasks.queue o
                WHERE o.topic_id = :topic::integer AND o.state = 'failed'
                  AND (:all::boolean OR o.task_id = ANY (:ids::bigint[]))
                ORDER BY o.task_id
                LIMIT :limit::integer
                FOR UPDATE OF o SKIP LOCKED
            )""";
        // The statement trigger on tasks.queue wakes the workers.
        revive = sql(pickDeadLetters + """
            UPDATE tasks.queue q
            SET state = 'ready', attempt = 0, run_at = NULL
            WHERE q.ctid = ANY (ARRAY(SELECT row FROM picked))
            RETURNING q.task_id
            """);
        discard = sql(pickDeadLetters + """
            , gone AS (
                DELETE FROM tasks.queue q
                WHERE q.ctid = ANY (ARRAY(SELECT row FROM picked))
                RETURNING q.task_id, q.attempt
            )
            UPDATE tasks.task t
            SET outcome = 'failed', attempts = gone.attempt, completed_at = now()
            FROM gone
            WHERE t.topic_id = :topic::integer AND t.id = gone.task_id
            RETURNING t.id
            """);
        // A discarded dead letter may have held back later tasks; no trigger sees a delete.
        notifyReady = sql("SELECT pg_notify('tasks_ready', :topic::text)");
        countQueue = sql("""
            SELECT count(*) FILTER (WHERE state = 'scheduled'),
                   count(*) FILTER (WHERE state = 'retrying'),
                   count(*) FILTER (WHERE state = 'ready'),
                   count(*) FILTER (WHERE state = 'active'),
                   count(*) FILTER (WHERE state = 'suspended'),
                   count(*) FILTER (WHERE state = 'failed'),
                   count(*) FILTER (WHERE state IN ('scheduled', 'retrying') AND run_at <= clock_timestamp()),
                   min(run_at),
                   (SELECT created_at FROM tasks.task WHERE id = min(task_id) FILTER (WHERE state = 'ready')) AS oldest_ready_at
            FROM tasks.queue
            WHERE topic_id = :topic::integer
            """);
        // Materialized for the same reason as promote: otherwise the limit doesn't hold. One range
        // of task_completed_idx (topic_id, outcome, completed_at).
        purge = sql("""
            WITH doomed AS MATERIALIZED (
                SELECT d.id FROM tasks.task d
                WHERE d.topic_id = :topic::integer AND d.outcome = :outcome::tasks.task_outcome AND d.completed_at < :before::timestamptz
                ORDER BY d.completed_at
                LIMIT :limit::integer
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM tasks.task t
            USING doomed
            WHERE t.topic_id = :topic::integer AND t.id = doomed.id
            """);
        // Written at every start, so it skips the write when nothing changed.
        setRetention = sql("""
            UPDATE tasks.topic
            SET retention = jsonb_object(:outcomes::text[], :periods::text[])
            WHERE id = :topic::integer
              AND retention IS DISTINCT FROM jsonb_object(:outcomes::text[], :periods::text[])
            """);
        retention = sql("""
            SELECT t.id, r.key, r.value
            FROM tasks.topic t
            CROSS JOIN LATERAL jsonb_each_text(t.retention) AS r
            ORDER BY t.id
            """);
        limitOverrides = sql("""
            SELECT name, rate, burst, until, version, updated_at, updated_by
            FROM tasks.rate_limit
            ORDER BY name
            """);
        // Only a limit some node configured (it has a token bucket) can be overridden. A first
        // override is based on version 0; two at once serialize on the primary key, and the second
        // then finds version 1 and changes nothing.
        overrideLimit = sql("""
            INSERT INTO tasks.rate_limit AS l (name, rate, burst, until, updated_by)
            SELECT a.name, :rate::double precision, :burst::integer, :until::timestamptz, :by::text
            FROM tasks.rate_admission a
            WHERE a.name = :name
              AND (coalesce(:expected::bigint, 0) = 0 OR EXISTS (SELECT 1 FROM tasks.rate_limit e WHERE e.name = :name))
            ON CONFLICT (name) DO UPDATE
            SET rate = excluded.rate, burst = excluded.burst, until = excluded.until,
                version = l.version + 1, updated_at = now(), updated_by = excluded.updated_by
            WHERE :expected::bigint IS NULL OR l.version = :expected::bigint
            RETURNING l.name, l.rate, l.burst, l.until, l.version, l.updated_at, l.updated_by
            """);
        limitConfigured = sql("SELECT EXISTS (SELECT 1 FROM tasks.rate_admission WHERE name = :name)");
        snapshot = sql("""
            INSERT INTO tasks.topic_count (topic_id, taken_at, scheduled, retrying, ready, active, suspended, failed, due, next_run_at,
                                           oldest_ready_at)
            SELECT t.id, now(),
                   count(q.task_id) FILTER (WHERE q.state = 'scheduled'),
                   count(q.task_id) FILTER (WHERE q.state = 'retrying'),
                   count(q.task_id) FILTER (WHERE q.state = 'ready'),
                   count(q.task_id) FILTER (WHERE q.state = 'active'),
                   count(q.task_id) FILTER (WHERE q.state = 'suspended'),
                   count(q.task_id) FILTER (WHERE q.state = 'failed'),
                   count(q.task_id) FILTER (WHERE q.state IN ('scheduled', 'retrying') AND q.run_at <= now()),
                   min(q.run_at),
                   (SELECT created_at FROM tasks.task WHERE id = min(q.task_id) FILTER (WHERE q.state = 'ready'))
            FROM tasks.topic t
            LEFT JOIN tasks.queue q ON q.topic_id = t.id
            GROUP BY t.id
            ON CONFLICT (topic_id) DO UPDATE SET taken_at = excluded.taken_at, scheduled = excluded.scheduled, retrying = excluded.retrying,
                ready = excluded.ready, active = excluded.active, suspended = excluded.suspended, failed = excluded.failed,
                due = excluded.due, next_run_at = excluded.next_run_at, oldest_ready_at = excluded.oldest_ready_at
            """);
        // Takes a lease nobody holds or whose term ran out, or renews this node's own. Two nodes
        // asking at once serialize on the row; the second sees the first's fresh term and gets nothing.
        lead = sql("""
            INSERT INTO tasks.lease AS l (name, node_id, acquired_at, expires_at)
            VALUES (:name, :node::integer, now(), now() + :term::bigint * interval '1 millisecond')
            ON CONFLICT (name) DO UPDATE
            SET node_id = excluded.node_id,
                acquired_at = CASE WHEN l.node_id = excluded.node_id THEN l.acquired_at ELSE excluded.acquired_at END,
                expires_at = excluded.expires_at
            WHERE l.node_id = excluded.node_id OR l.expires_at < now()
            RETURNING l.expires_at
            """);
        resign = sql("DELETE FROM tasks.lease WHERE name = :name AND node_id = :node::integer");
        snapshots = sql("""
            SELECT tp.name, c.scheduled, c.retrying, c.ready, c.active, c.suspended, c.failed, c.due, c.next_run_at, c.taken_at,
                   c.oldest_ready_at
            FROM tasks.topic_count c
            JOIN tasks.topic tp ON tp.id = c.topic_id
            ORDER BY tp.name
            """);
        activeClaims = sql("""
            SELECT topic_id, task_id, attempt, claimed_at
            FROM tasks.queue
            WHERE node_id = :node::integer AND state = 'active'
            ORDER BY task_id
            """);
        listAttempts = sql("""
            SELECT task_id, attempt, node_id, result, error::text
            FROM tasks.attempt
            WHERE task_id = :id::bigint
            ORDER BY attempt, finished_at
            """);
    }


    @Override
    public void prepare() throws SQLException {
        TaskSchema.prepare(dataSource, mode);
    }

    @Override
    public StoreTransaction begin() throws SQLException {
        return PostgresTransaction.open(dataSource.getConnection());
    }

    @Override
    public boolean joinsApplicationTransactions() {
        return true;
    }

    @Override
    public StoreTransaction join(Connection connection) {
        return PostgresTransaction.join(connection);
    }

    private static NamedSql sql(String text) {
        return NamedSql.parse(text);
    }

    private static String pushSql(Insertion insertion) {
        // One statement: ids are drawn in input order, queue rows are inserted first so a dedup
        // conflict also skips the task row, then older rows are handled for the identifiers
        // that were actually queued. Every CTE sees the snapshot from before the statement, so
        // none of them can touch the rows this statement inserts.
        //
        // Older queue rows are deleted by task_id, and also by topic and identifier, which never
        // change: by task_id alone, a planner that believes the queue is empty may look them up in
        // queue_identifier_idx, where task_id is the last column, and walk the whole index on every
        // push (found by the drained-queue pass of the TCK). History is deleted by one array of
        // ids: with an OR of two IN lists, the only index condition left was the topic, so every
        // DELETE push read the topic's whole history.
        String cleanup = switch (insertion) {
            case APPEND -> "";
            case SUPERSEDE -> """
                ,
                superseded AS (
                    DELETE FROM tasks.queue q
                    WHERE q.topic_id = :topic::integer
                      AND q.identifier = ANY (ARRAY(SELECT identifier FROM queued))
                      AND q.task_id = ANY (ARRAY(
                        SELECT o.task_id FROM tasks.queue o
                        WHERE o.topic_id = :topic::integer
                          AND o.identifier = ANY (ARRAY(SELECT identifier FROM queued))
                          AND o.state IN ('ready', 'scheduled', 'retrying')
                        FOR UPDATE OF o SKIP LOCKED))
                    RETURNING q.task_id
                ),
                superseded_tasks AS (
                    UPDATE tasks.task t SET outcome = 'redundant', completed_at = now()
                    WHERE t.topic_id = :topic::integer AND t.id IN (SELECT task_id FROM superseded)
                )
                """;
            case REPLACE -> """
                ,
                replaced AS (
                    DELETE FROM tasks.queue q
                    WHERE q.topic_id = :topic::integer
                      AND q.identifier = ANY (ARRAY(SELECT identifier FROM queued))
                      AND q.task_id = ANY (ARRAY(
                        SELECT o.task_id FROM tasks.queue o
                        WHERE o.topic_id = :topic::integer
                          AND o.identifier = ANY (ARRAY(SELECT identifier FROM queued))
                          AND o.state <> 'active'
                        FOR UPDATE OF o SKIP LOCKED))
                    RETURNING q.task_id
                ),
                replaced_open AS (
                    UPDATE tasks.task t SET outcome = 'redundant', completed_at = now()
                    WHERE t.topic_id = :topic::integer AND t.id IN (SELECT task_id FROM replaced)
                ),
                replaced_done AS (
                    UPDATE tasks.task t SET previous_outcome = t.outcome, outcome = 'redundant'
                    WHERE t.topic_id = :topic::integer
                      AND t.id IN (
                        SELECT d.id FROM tasks.task d
                        WHERE d.topic_id = :topic::integer
                          AND d.identifier IN (SELECT identifier FROM queued)
                          AND d.outcome IN ('succeeded', 'filtered', 'failed')
                        FOR UPDATE OF d SKIP LOCKED)
                )
                """;
            case DELETE -> """
                ,
                deleted AS (
                    DELETE FROM tasks.queue q
                    WHERE q.topic_id = :topic::integer
                      AND q.identifier = ANY (ARRAY(SELECT identifier FROM queued))
                      AND q.task_id = ANY (ARRAY(
                        SELECT o.task_id FROM tasks.queue o
                        WHERE o.topic_id = :topic::integer
                          AND o.identifier = ANY (ARRAY(SELECT identifier FROM queued))
                          AND o.state <> 'active'
                        FOR UPDATE OF o SKIP LOCKED))
                    RETURNING q.task_id
                ),
                deleted_tasks AS (
                    DELETE FROM tasks.task t
                    WHERE t.topic_id = :topic::integer
                      AND t.id = ANY (ARRAY(SELECT task_id FROM deleted) || ARRAY(
                          SELECT d.id FROM tasks.task d
                          WHERE d.topic_id = :topic::integer
                            AND d.identifier IN (SELECT identifier FROM queued)
                            AND d.outcome IS NOT NULL
                          FOR UPDATE OF d SKIP LOCKED))
                )
                """;
        };
        return """
            WITH input AS MATERIALIZED (
                SELECT nextval('tasks.task_id_seq') AS id, i.*
                FROM unnest(:identifiers::text[], :payloads::text[], :types::text[], :references::text[],
                            :parents::bigint[], :roots::bigint[], :states::text[], :delays::bigint[],
                            :priorities::integer[], :dedup::text[], :traces::text[])
                    WITH ORDINALITY AS i(identifier, payload, payload_type, reference, parent_id, root_id,
                                         state, delay_ms, priority, dedup_key, trace_context, ord)
            ),
            queued AS (
                INSERT INTO tasks.queue (task_id, topic_id, identifier, state, priority, run_at, dedup_key)
                SELECT id, :topic::integer, identifier, state::tasks.queue_state, priority,
                       CASE WHEN state = 'scheduled' THEN now() + delay_ms * interval '1 millisecond' END,
                       dedup_key
                FROM input
                WHERE state IS NOT NULL
                ORDER BY ord
                ON CONFLICT (topic_id, dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING
                RETURNING task_id, identifier
            ),
            stored AS (
                INSERT INTO tasks.task (topic_id, id, identifier, reference, payload, payload_type, parent_id, root_id, trace_context,
                                        outcome, completed_at)
                SELECT :topic::integer, id, identifier, reference, payload, payload_type, parent_id, root_id, trace_context,
                       CASE WHEN state IS NULL THEN 'redundant'::tasks.task_outcome END,
                       CASE WHEN state IS NULL THEN now() END
                FROM input
                WHERE state IS NULL OR id IN (SELECT task_id FROM queued)
                ORDER BY ord
            )
            """ + cleanup + """
            SELECT id, state IS NULL AS redundant, id IN (SELECT task_id FROM queued) AS queued
            FROM input
            ORDER BY ord
            """;
    }

    private static String junctionSql(Juncture juncture) {
        // A group is complete when every identifier has a task that counts; then the group's
        // suspended tasks become ready.
        String counts = switch (juncture) {
            case SINGULAR -> """
                SELECT 1 FROM tasks.queue s
                WHERE s.topic_id = :topic::integer AND s.identifier = g.identifier AND s.state = 'suspended'
                """;
            case ETERNAL -> """
                SELECT 1 FROM tasks.task t
                WHERE t.topic_id = :topic::integer AND t.identifier = g.identifier
                  AND (t.outcome IS NULL OR t.outcome <> 'redundant')
                """;
        };
        return """
            WITH g AS (
                SELECT * FROM unnest(:groups::integer[], :identifiers::text[]) AS g(group_no, identifier)
            ),
            checked AS (
                SELECT g.group_no, g.identifier, EXISTS (%s) AS present
                FROM g
            ),
            complete AS (
                SELECT group_no FROM checked GROUP BY group_no HAVING bool_and(present)
            ),
            released AS (
                UPDATE tasks.queue q
                SET state = 'ready'
                WHERE q.topic_id = :topic::integer
                  AND q.state = 'suspended'
                  AND q.identifier = ANY (ARRAY(SELECT c.identifier FROM checked c JOIN complete USING (group_no)))
                RETURNING q.identifier
            )
            SELECT DISTINCT identifier FROM released
            """.formatted(counts);
    }

    private static String pollSql(Ordering ordering) {
        String blocking = switch (ordering) {
            case FIFO -> null;
            case SINGULAR_BY_IDENTIFIER -> "'ready', 'retrying'";
            case SINGULAR_BY_IDENTIFIER_ON_FAILURE -> "'ready', 'retrying', 'failed'";
            case SINGULAR_BY_IDENTIFIER_UNTIL_SUCCESS -> "'ready', 'retrying', 'failed', 'suspended', 'scheduled'";
        };
        // Only tasks in the same topic count (finding #6), and only earlier ones (finding #7),
        // except that an active task blocks its identifier in both directions.
        //
        // The check must be one lookup of the candidate's identifier in queue_identifier_idx,
        // whatever the statistics say:
        // - OFFSET 0 keeps it a subplan run per candidate, with the identifier as a parameter.
        //   As a NOT EXISTS anti-join the planner may scan the whole topic per candidate instead,
        //   and does when the statistics say the queue is empty, which they do whenever
        //   autovacuum analyzed it right after a burst drained (ClaimPlanTest: about 30 s
        //   instead of 2 for 20,000 tasks).
        // - CASE instead of OR: an OR on the state lets the planner add a bitmap arm that scans
        //   queue_active_idx, every active task of every topic, once per candidate (the Phase 2
        //   benchmark's SINGULAR scenario: 13 ms claims, 5 times slower).
        String condition = blocking == null ? "" : """
                  AND NOT EXISTS (
                      SELECT 1 FROM tasks.queue b
                      WHERE b.topic_id = c.topic_id
                        AND b.identifier = c.identifier
                        AND b.task_id <> c.task_id
                        AND CASE WHEN b.state = 'active' THEN true
                                 WHEN b.task_id < c.task_id THEN b.state IN (%s)
                                 ELSE false END
                      OFFSET 0)
            """.formatted(blocking);
        return """
            WITH picked AS MATERIALIZED (
                SELECT c.ctid AS row
                FROM tasks.queue c
                WHERE c.topic_id = :topic::integer
                  AND c.state = 'ready'
            """ + condition + """
                ORDER BY c.priority DESC, c.task_id
                LIMIT :limit::integer
                FOR UPDATE OF c SKIP LOCKED
            ),
            claimed AS (
                UPDATE tasks.queue q
                SET state = 'active', node_id = :node::integer, attempt = q.attempt + 1, claimed_at = clock_timestamp()
                WHERE q.ctid = ANY (ARRAY(SELECT row FROM picked))
                RETURNING q.task_id, q.identifier, q.attempt, q.priority, q.claimed_at
            )
            SELECT c.task_id, c.identifier, c.attempt, c.priority, c.claimed_at,
                   t.payload, t.payload_type, t.reference, t.parent_id, t.root_id, t.created_at, t.trace_context
            FROM claimed c
            JOIN tasks.task t ON t.id = c.task_id
            WHERE t.topic_id = :topic::integer
            ORDER BY c.priority DESC, c.task_id
            """;
    }

    @Override
    public int createTopic(StoreTransaction transaction, String name) throws SQLException {
        Connection connection = transaction.connection();
        try (PreparedStatement statement = createTopic.prepare(connection, NamedSql.parameters().set("name", name));
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    @Override
    public OptionalInt findTopic(StoreTransaction transaction, String name) throws SQLException {
        Connection connection = transaction.connection();
        try (PreparedStatement statement = findTopic.prepare(connection, NamedSql.parameters().set("name", name));
             ResultSet resultSet = statement.executeQuery()) {
            return resultSet.next() ? OptionalInt.of(resultSet.getInt(1)) : OptionalInt.empty();
        }
    }

    @Override
    public List<TopicInfo> topics(StoreTransaction transaction) throws SQLException {
        List<TopicInfo> topics = new ArrayList<>();
        try (PreparedStatement statement = listTopics.prepare(transaction.connection(), NamedSql.parameters());
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                topics.add(new TopicInfo(resultSet.getInt(1), resultSet.getString(2), resultSet.getBoolean(3)));
            }
        }
        return topics;
    }

    @Override
    public boolean setPaused(StoreTransaction transaction, String name, boolean paused) throws SQLException {
        try (PreparedStatement statement = pauseTopic.prepare(transaction.connection(), NamedSql.parameters().set("name", name).set("paused", paused));
             ResultSet resultSet = statement.executeQuery()) {
            return resultSet.next();
        }
    }

    @Override
    public List<Pushed> push(StoreTransaction transaction, int topicId, Insertion insertion, List<NewTask> tasks) throws SQLException {
        Connection connection = transaction.connection();
        if (tasks.isEmpty()) {
            return List.of();
        }
        // Within one push, SUPERSEDE and REPLACE keep earlier duplicates as redundant history;
        // DELETE drops them.
        Map<String, Integer> last = new HashMap<>();
        for (int index = 0; index < tasks.size(); index++) {
            last.put(tasks.get(index).identifier(), index);
        }
        List<NewTask> stored = new ArrayList<>(tasks.size());
        List<String> states = new ArrayList<>(tasks.size());
        boolean[] dropped = new boolean[tasks.size()];
        for (int index = 0; index < tasks.size(); index++) {
            NewTask task = tasks.get(index);
            boolean superseded = insertion != Insertion.APPEND && last.get(task.identifier()) != index;
            if (superseded && insertion == Insertion.DELETE) {
                dropped[index] = true;
                continue;
            }
            stored.add(task);
            states.add(superseded ? null : task.suspended() ? "suspended" : task.delay() != null && !task.delay().isZero() ? "scheduled" : "ready");
        }
        int size = stored.size();
        Object[] identifiers = new Object[size], payloads = new Object[size], types = new Object[size], references = new Object[size];
        Object[] parents = new Object[size], roots = new Object[size], delays = new Object[size], priorities = new Object[size], dedup = new Object[size];
        Object[] traces = new Object[size];
        for (int index = 0; index < size; index++) {
            NewTask task = stored.get(index);
            identifiers[index] = task.identifier();
            payloads[index] = task.payload();
            types[index] = task.payloadType();
            references[index] = task.reference();
            parents[index] = task.parentId();
            roots[index] = task.rootId();
            delays[index] = task.delay() == null ? null : task.delay().toMillis();
            priorities[index] = task.priority();
            dedup[index] = task.dedupKey();
            traces[index] = task.traceContext();
        }
        NamedSql.Parameters parameters = NamedSql.parameters()
            .set("topic", topicId)
            .array("identifiers", "text", identifiers)
            .array("payloads", "text", payloads)
            .array("types", "text", types)
            .array("references", "text", references)
            .array("parents", "bigint", parents)
            .array("roots", "bigint", roots)
            .array("states", "text", states.toArray())
            .array("delays", "bigint", delays)
            .array("priorities", "integer", priorities)
            .array("dedup", "text", dedup)
            .array("traces", "text", traces);
        List<Pushed> results = new ArrayList<>(tasks.size());
        try (PreparedStatement statement = pushes.get(insertion).prepare(connection, parameters);
             ResultSet resultSet = statement.executeQuery()) {
            for (int index = 0; index < tasks.size(); index++) {
                String identifier = tasks.get(index).identifier();
                if (dropped[index]) {
                    results.add(new Pushed(identifier, Pushed.Status.REDUNDANT, null));
                    continue;
                }
                if (!resultSet.next()) {
                    throw new IllegalStateException("Push returned fewer rows than it was given");
                }
                long id = resultSet.getLong("id");
                if (resultSet.getBoolean("redundant")) {
                    results.add(new Pushed(identifier, Pushed.Status.REDUNDANT, id));
                } else if (resultSet.getBoolean("queued")) {
                    results.add(new Pushed(identifier, Pushed.Status.QUEUED, id));
                } else {
                    results.add(new Pushed(identifier, Pushed.Status.DUPLICATE, null));
                }
            }
        }
        return results;
    }

    /**
     * Claims in the given transaction, which the engine starts for the claim alone: the claim sets
     * transaction-local planner settings that pin its plan (see {@code claimPlanning}).
     */
    @Override
    public Claim claim(StoreTransaction transaction, ClaimRequest request) throws SQLException {
        Connection connection = transaction.connection();
        try (PreparedStatement statement = claimPlanning.prepare(connection, NamedSql.parameters())) {
            statement.execute();
        }
        Admission admission = request.admission();
        int limit = request.limit();
        if (admission != null) {
            double available = available(connection, admission);
            int permits = (int) Math.min(limit, Math.floor(available));
            if (permits < 1) {
                long waitNanos = (long) Math.ceil((1 - available) / admission.perSecond() * 1_000_000_000L);
                return new Claim(List.of(), Duration.ofNanos(Math.max(waitNanos, 1_000_000L)));
            }
            limit = permits;
        }
        List<ClaimedTask> tasks = new ArrayList<>();
        NamedSql.Parameters parameters = NamedSql.parameters()
            .set("topic", request.topicId())
            .set("node", request.nodeId())
            .set("limit", limit);
        try (PreparedStatement statement = polls.get(request.ordering()).prepare(connection, parameters);
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                tasks.add(new ClaimedTask(
                    resultSet.getLong("task_id"),
                    resultSet.getString("identifier"),
                    resultSet.getInt("attempt"),
                    resultSet.getInt("priority"),
                    resultSet.getString("payload"),
                    resultSet.getString("payload_type"),
                    resultSet.getString("reference"),
                    resultSet.getObject("parent_id", Long.class),
                    resultSet.getObject("root_id", Long.class),
                    instant(resultSet, "created_at"),
                    instant(resultSet, "claimed_at"),
                    resultSet.getString("trace_context")
                ));
            }
        }
        if (admission != null && !tasks.isEmpty()) {
            NamedSql.Parameters charge = NamedSql.parameters()
                .set("name", admission.name())
                .set("burst", admission.burst())
                .set("rate", admission.perSecond())
                .set("claimed", tasks.size());
            try (PreparedStatement statement = admissionCharge.prepare(connection, charge)) {
                statement.executeUpdate();
            }
        }
        return tasks.isEmpty() ? Claim.EMPTY : new Claim(List.copyOf(tasks), null);
    }

    private double available(Connection connection, Admission admission) throws SQLException {
        NamedSql.Parameters parameters = NamedSql.parameters()
            .set("name", admission.name())
            .set("burst", admission.burst())
            .set("rate", admission.perSecond());
        for (int round = 0; round < 2; round++) {
            try (PreparedStatement statement = admissionLock.prepare(connection, parameters);
                 ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getDouble(1);
                }
            }
            insertAdmission(connection, admission.name(), admission.burst());
        }
        throw new IllegalStateException("No admission state for rate limit " + admission.name());
    }

    @Override
    public void complete(StoreTransaction transaction, int topicId, int nodeId, List<Completion> completions) throws SQLException {
        Connection connection = transaction.connection();
        Set<Long> matched = apply(connection, topicId, nodeId, completions);
        if (matched.size() != completions.size()) {
            Set<Long> lost = new LinkedHashSet<>();
            completions.forEach(completion -> {
                if (!matched.contains(completion.taskId())) {
                    lost.add(completion.taskId());
                }
            });
            throw new StaleClaimException(lost);
        }
    }

    @Override
    public int release(StoreTransaction transaction, int topicId, int nodeId, List<Completion> completions) throws SQLException {
        Connection connection = transaction.connection();
        return apply(connection, topicId, nodeId, completions).size();
    }

    private Set<Long> apply(Connection connection, int topicId, int nodeId, List<Completion> completions) throws SQLException {
        List<Completion> finished = new ArrayList<>(), waiting = new ArrayList<>();
        Set<Long> unique = new HashSet<>();
        for (Completion completion : completions) {
            if (!unique.add(completion.taskId())) {
                throw new IllegalArgumentException("Task " + completion.taskId() + " is completed twice");
            }
            (completion.resolution() instanceof Resolution.Finished ? finished : waiting).add(completion);
        }
        Set<Long> matched = new HashSet<>();
        if (!finished.isEmpty()) {
            int size = finished.size();
            Object[] ids = new Object[size], attempts = new Object[size], outcomes = new Object[size], outputs = new Object[size];
            for (int index = 0; index < size; index++) {
                Completion completion = finished.get(index);
                Resolution.Finished resolution = (Resolution.Finished) completion.resolution();
                ids[index] = completion.taskId();
                attempts[index] = completion.attempt();
                outcomes[index] = switch (resolution.outcome()) {
                    case SUCCEEDED -> "succeeded";
                    case FILTERED -> "filtered";
                };
                outputs[index] = resolution.output();
            }
            NamedSql.Parameters parameters = NamedSql.parameters()
                .set("topic", topicId)
                .set("node", nodeId)
                .array("ids", "bigint", ids)
                .array("attempts", "integer", attempts)
                .array("outcomes", "text", outcomes)
                .array("outputs", "text", outputs);
            collect(finish, connection, parameters, matched);
        }
        if (!waiting.isEmpty()) {
            int size = waiting.size();
            Object[] ids = new Object[size], attempts = new Object[size], states = new Object[size], delays = new Object[size];
            Object[] uncounted = new Object[size], errors = new Object[size], started = new Object[size];
            for (int index = 0; index < size; index++) {
                Completion completion = waiting.get(index);
                Resolution.Waiting resolution = (Resolution.Waiting) completion.resolution();
                ids[index] = completion.taskId();
                attempts[index] = completion.attempt();
                states[index] = switch (resolution.state()) {
                    case SCHEDULED -> "scheduled";
                    case RETRYING -> "retrying";
                    case SUSPENDED -> "suspended";
                    case DEAD_LETTER -> "failed";
                };
                delays[index] = resolution.delay() == null ? null : resolution.delay().toMillis();
                uncounted[index] = resolution.counted() ? 0 : 1;
                errors[index] = resolution.error();
                started[index] = completion.claimedAt() == null ? null : completion.claimedAt().toString();
            }
            NamedSql.Parameters parameters = NamedSql.parameters()
                .set("topic", topicId)
                .set("node", nodeId)
                .array("ids", "bigint", ids)
                .array("attempts", "integer", attempts)
                .array("states", "text", states)
                .array("delays", "bigint", delays)
                .array("uncounted", "integer", uncounted)
                .array("errors", "text", errors)
                .array("started", "text", started);
            collect(wait, connection, parameters, matched);
        }
        return matched;
    }

    private static void collect(NamedSql sql, Connection connection, NamedSql.Parameters parameters, Set<Long> matched) throws SQLException {
        try (PreparedStatement statement = sql.prepare(connection, parameters);
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                matched.add(resultSet.getLong(1));
            }
        }
    }

    @Override
    public List<Integer> promoteDue(StoreTransaction transaction, int limit) throws SQLException {
        Connection connection = transaction.connection();
        return topics(promote, connection, NamedSql.parameters().set("limit", limit));
    }

    @Override
    public List<Integer> requeueExpired(StoreTransaction transaction, int nodeId, Duration expiry) throws SQLException {
        Connection connection = transaction.connection();
        return topics(expire, connection, NamedSql.parameters().set("node", nodeId).set("expiry", expiry.toMillis()));
    }

    private static List<Integer> topics(NamedSql sql, Connection connection, NamedSql.Parameters parameters) throws SQLException {
        Set<Integer> topics = new LinkedHashSet<>();
        try (PreparedStatement statement = sql.prepare(connection, parameters);
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                topics.add(resultSet.getInt(1));
            }
        }
        return List.copyOf(topics);
    }

    @Override
    public boolean lead(StoreTransaction transaction, String name, int nodeId, Duration term) throws SQLException {
        NamedSql.Parameters parameters = NamedSql.parameters().set("name", name).set("node", nodeId).set("term", term.toMillis());
        try (PreparedStatement statement = lead.prepare(transaction.connection(), parameters);
             ResultSet resultSet = statement.executeQuery()) {
            return resultSet.next();
        }
    }

    @Override
    public void resign(StoreTransaction transaction, String name, int nodeId) throws SQLException {
        try (PreparedStatement statement = resign.prepare(transaction.connection(), NamedSql.parameters().set("name", name).set("node", nodeId))) {
            statement.executeUpdate();
        }
    }

    @Override
    public int purge(StoreTransaction transaction, int topicId, org.example.task2.Outcome outcome, Instant completedBefore, int limit) throws SQLException {
        NamedSql.Parameters parameters = NamedSql.parameters()
            .set("topic", topicId)
            .set("outcome", outcome.label())
            .set("before", OffsetDateTime.ofInstant(completedBefore, java.time.ZoneOffset.UTC))
            .set("limit", limit);
        try (PreparedStatement statement = purge.prepare(transaction.connection(), parameters)) {
            return statement.executeUpdate();
        }
    }

    @Override
    public void setRetention(StoreTransaction transaction, int topicId, Map<org.example.task2.Outcome, Period> retention) throws SQLException {
        List<org.example.task2.Outcome> outcomes = retention.keySet().stream().sorted().toList();
        NamedSql.Parameters parameters = NamedSql.parameters()
            .set("topic", topicId)
            .array("outcomes", "text", outcomes.stream().map(org.example.task2.Outcome::label).toArray())
            .array("periods", "text", outcomes.stream().map(outcome -> retention.get(outcome).toString()).toArray());
        try (PreparedStatement statement = setRetention.prepare(transaction.connection(), parameters)) {
            statement.executeUpdate();
        }
    }

    @Override
    public Map<Integer, Map<org.example.task2.Outcome, Period>> retention(StoreTransaction transaction) throws SQLException {
        Map<Integer, Map<org.example.task2.Outcome, Period>> result = new LinkedHashMap<>();
        try (PreparedStatement statement = retention.prepare(transaction.connection(), NamedSql.parameters());
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                // Skips outcomes this version doesn't know, set by a newer node during a rolling deploy.
                String label = resultSet.getString(2);
                Optional<org.example.task2.Outcome> outcome = Arrays.stream(org.example.task2.Outcome.values())
                    .filter(candidate -> candidate.label().equals(label))
                    .findFirst();
                if (outcome.isPresent()) {
                    result.computeIfAbsent(resultSet.getInt(1), ignored -> new EnumMap<>(org.example.task2.Outcome.class))
                        .put(outcome.get(), Period.parse(resultSet.getString(3)));
                }
            }
        }
        return result;
    }

    @Override
    public void snapshotCounts(StoreTransaction transaction) throws SQLException {
        try (PreparedStatement statement = snapshot.prepare(transaction.connection(), NamedSql.parameters())) {
            statement.executeUpdate();
        }
    }

    @Override
    public List<TopicSnapshot> snapshots(StoreTransaction transaction) throws SQLException {
        List<TopicSnapshot> result = new ArrayList<>();
        try (PreparedStatement statement = snapshots.prepare(transaction.connection(), NamedSql.parameters());
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                result.add(new TopicSnapshot(resultSet.getString(1), new QueueCounts(resultSet.getLong(2), resultSet.getLong(3), resultSet.getLong(4),
                    resultSet.getLong(5), resultSet.getLong(6), resultSet.getLong(7), resultSet.getLong(8), instant(resultSet, "next_run_at"),
                    instant(resultSet, "oldest_ready_at")),
                    instant(resultSet, "taken_at")));
            }
        }
        return result;
    }

    @Override
    public List<ActiveClaim> activeClaims(StoreTransaction transaction, int nodeId) throws SQLException {
        List<ActiveClaim> claims = new ArrayList<>();
        try (PreparedStatement statement = activeClaims.prepare(transaction.connection(), NamedSql.parameters().set("node", nodeId));
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                claims.add(new ActiveClaim(resultSet.getInt(1), resultSet.getLong(2), resultSet.getInt(3), instant(resultSet, "claimed_at")));
            }
        }
        return claims;
    }

    @Override
    public int registerNode(StoreTransaction transaction, String name, String version) throws SQLException {
        Connection connection = transaction.connection();
        try (PreparedStatement statement = registerNode.prepare(connection, NamedSql.parameters().set("name", name).set("version", version));
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    @Override
    public void heartbeat(StoreTransaction transaction, int nodeId) throws SQLException {
        Connection connection = transaction.connection();
        try (PreparedStatement statement = heartbeat.prepare(connection, NamedSql.parameters().set("node", nodeId))) {
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Node " + nodeId + " is not registered");
            }
        }
    }

    @Override
    public void ensureAdmission(StoreTransaction transaction, String name, int burst) throws SQLException {
        insertAdmission(transaction.connection(), name, burst);
    }

    private void insertAdmission(Connection connection, String name, int burst) throws SQLException {
        try (PreparedStatement statement = ensureAdmission.prepare(connection, NamedSql.parameters().set("name", name).set("burst", (double) burst))) {
            statement.executeUpdate();
        }
    }

    @Override
    public Set<String> junction(StoreTransaction transaction, int topicId, Juncture juncture, Collection<Set<String>> groups) throws SQLException {
        List<Object> numbers = new ArrayList<>(), identifiers = new ArrayList<>();
        int group = 0;
        for (Set<String> members : groups) {
            group++;
            for (String identifier : members) {
                numbers.add(group);
                identifiers.add(identifier);
            }
        }
        if (identifiers.isEmpty()) {
            return Set.of();
        }
        NamedSql.Parameters parameters = NamedSql.parameters()
            .set("topic", topicId)
            .array("groups", "integer", numbers.toArray())
            .array("identifiers", "text", identifiers.toArray());
        Set<String> released = new LinkedHashSet<>();
        try (PreparedStatement statement = junctions.get(juncture).prepare(transaction.connection(), parameters);
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                released.add(resultSet.getString(1));
            }
        }
        return released;
    }

    @Override
    public List<LimitOverride> limitOverrides(StoreTransaction transaction) throws SQLException {
        List<LimitOverride> overrides = new ArrayList<>();
        try (PreparedStatement statement = limitOverrides.prepare(transaction.connection(), NamedSql.parameters());
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                overrides.add(limitOverride(resultSet));
            }
        }
        return overrides;
    }

    @Override
    public Optional<LimitOverride> overrideLimit(StoreTransaction transaction, String name, Double perSecond, Integer burst, Instant until, String by,
                                                 Long expectedVersion) throws SQLException {
        // Validates the values the way the row will hold them, before touching the database.
        new LimitOverride(name, perSecond, burst, until, 0, null, by);
        NamedSql.Parameters parameters = NamedSql.parameters()
            .set("name", name)
            .set("rate", perSecond)
            .set("burst", burst)
            .set("until", until == null ? null : OffsetDateTime.ofInstant(until, java.time.ZoneOffset.UTC))
            .set("by", by)
            .set("expected", expectedVersion);
        try (PreparedStatement statement = overrideLimit.prepare(transaction.connection(), parameters);
             ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
                return Optional.of(limitOverride(resultSet));
            }
        }
        try (PreparedStatement statement = limitConfigured.prepare(transaction.connection(), NamedSql.parameters().set("name", name));
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            if (!resultSet.getBoolean(1)) {
                throw new IllegalArgumentException("No node has configured a rate limit named '" + name + "'");
            }
        }
        return Optional.empty();
    }

    private static LimitOverride limitOverride(ResultSet resultSet) throws SQLException {
        return new LimitOverride(
            resultSet.getString("name"),
            resultSet.getObject("rate", Double.class),
            resultSet.getObject("burst", Integer.class),
            instant(resultSet, "until"),
            resultSet.getLong("version"),
            instant(resultSet, "updated_at"),
            resultSet.getString("updated_by")
        );
    }

    @Override
    public Optional<TaskView> find(StoreTransaction transaction, long taskId) throws SQLException {
        try (PreparedStatement statement = findTask.prepare(transaction.connection(), NamedSql.parameters().set("id", taskId));
             ResultSet resultSet = statement.executeQuery()) {
            return resultSet.next() ? Optional.of(taskView(resultSet)) : Optional.empty();
        }
    }

    @Override
    public List<TaskView> deadLetters(StoreTransaction transaction, int topicId, long afterId, int limit) throws SQLException {
        List<TaskView> result = new ArrayList<>();
        try (PreparedStatement statement = deadLetters.prepare(transaction.connection(),
                 NamedSql.parameters().set("topic", topicId).set("after", afterId).set("limit", limit));
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                result.add(taskView(resultSet));
            }
        }
        return result;
    }

    @Override
    public List<Long> revive(StoreTransaction transaction, int topicId, Collection<Long> taskIds, int limit) throws SQLException {
        return deadLetterIds(revive, transaction, topicId, taskIds, limit);
    }

    @Override
    public List<Long> discard(StoreTransaction transaction, int topicId, Collection<Long> taskIds, int limit) throws SQLException {
        List<Long> discarded = deadLetterIds(discard, transaction, topicId, taskIds, limit);
        if (!discarded.isEmpty()) {
            try (PreparedStatement statement = notifyReady.prepare(transaction.connection(), NamedSql.parameters().set("topic", topicId))) {
                statement.execute();
            }
        }
        return discarded;
    }

    private static List<Long> deadLetterIds(NamedSql sql, StoreTransaction transaction, int topicId, Collection<Long> taskIds, int limit) throws SQLException {
        NamedSql.Parameters parameters = NamedSql.parameters()
            .set("topic", topicId)
            .set("all", taskIds == null)
            .array("ids", "bigint", taskIds == null ? new Object[0] : taskIds.toArray())
            .set("limit", taskIds == null ? limit : Math.min(limit, taskIds.size()));
        List<Long> ids = new ArrayList<>();
        try (PreparedStatement statement = sql.prepare(transaction.connection(), parameters);
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                ids.add(resultSet.getLong(1));
            }
        }
        ids.sort(null);
        return ids;
    }

    private static TaskView taskView(ResultSet resultSet) throws SQLException {
        return new TaskView(
            resultSet.getString("topic"),
            resultSet.getLong("id"),
            resultSet.getString("identifier"),
            resultSet.getString("state"),
            resultSet.getBoolean("finished"),
            resultSet.getInt("attempt"),
            instant(resultSet, "run_at"),
            resultSet.getInt("priority"),
            resultSet.getObject("node_id", Integer.class),
            resultSet.getString("reference"),
            resultSet.getObject("parent_id", Long.class),
            resultSet.getObject("root_id", Long.class),
            resultSet.getString("payload"),
            resultSet.getString("payload_type"),
            resultSet.getString("output"),
            resultSet.getString("error"),
            resultSet.getString("previous_outcome"),
            instant(resultSet, "created_at"),
            instant(resultSet, "completed_at")
        );
    }

    @Override
    public QueueCounts count(StoreTransaction transaction, int topicId) throws SQLException {
        try (PreparedStatement statement = countQueue.prepare(transaction.connection(), NamedSql.parameters().set("topic", topicId));
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return new QueueCounts(resultSet.getLong(1), resultSet.getLong(2), resultSet.getLong(3), resultSet.getLong(4),
                resultSet.getLong(5), resultSet.getLong(6), resultSet.getLong(7), instant(resultSet, "min"), instant(resultSet, "oldest_ready_at"));
        }
    }

    @Override
    public List<AttemptView> attempts(StoreTransaction transaction, long taskId) throws SQLException {
        List<AttemptView> attempts = new ArrayList<>();
        try (PreparedStatement statement = listAttempts.prepare(transaction.connection(), NamedSql.parameters().set("id", taskId));
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                attempts.add(new AttemptView(resultSet.getLong(1), resultSet.getInt(2), resultSet.getObject(3, Integer.class),
                    resultSet.getString(4), resultSet.getString(5)));
            }
        }
        return attempts;
    }

    private static Instant instant(ResultSet resultSet, String column) throws SQLException {
        OffsetDateTime value = resultSet.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    @Override
    public String toString() {
        return "PostgresTaskStore[" + dataSource + ", " + mode + "]";
    }
}
