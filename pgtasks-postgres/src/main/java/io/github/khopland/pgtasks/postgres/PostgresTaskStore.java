package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
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

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.SchemaMode;
import io.github.khopland.pgtasks.admin.DeadLetterQuery;
import io.github.khopland.pgtasks.admin.NodeView;
import io.github.khopland.pgtasks.admin.QueueCounts;
import io.github.khopland.pgtasks.admin.QueueState;
import io.github.khopland.pgtasks.admin.TaskQuery;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.admin.TopicSnapshot;
import io.github.khopland.pgtasks.spi.Notifier;
import io.github.khopland.pgtasks.spi.StaleClaimException;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.jspecify.annotations.Nullable;

/**
 * The task store on the pgtasks schema, {@value TaskSchema#DEFAULT_SCHEMA} unless named otherwise
 * (see {@link TaskSchema}). Every operation is one
 * statement, except a claim: it first pins its plan with transaction-local planner settings, and
 * a SINGULAR_* claim takes the topic's advisory lock. When an engine starts, the store migrates or
 * validates the schema according to its {@link SchemaMode}.
 */
public final class PostgresTaskStore implements TaskStore {

    /** The first key of the advisory lock SINGULAR_* claims take per topic; never a topic id, which is positive. */
    private static final int SINGULAR_CLAIM_LOCK = -0x7067_6c6b;

    private final DataSource dataSource;
    private final Duration networkTimeout;
    private final SchemaMode mode;
    private final String schema;
    private final DatabaseClock clock = new DatabaseClock();
    private final NamedSql createTopic, findTopic, listTopics, registerNode, heartbeat;
    private final NamedSql finish, wait, noteError, promote, expire, findTask, countQueue, activeClaims, settledClaims, purge, snapshot, snapshots, claimPlanning;
    private final NamedSql deadLetters, revive, discard, notifyReady, notifyChannel, purgeAll, lockTopic, topicHasTasks, deleteTopic;
    private final NamedSql searchTasks, releaseSuspended;
    private final NamedSql deleteDeadNodes, deregisterNode, listNodes;
    private final NamedSql lead, resign, setRetention, retention;
    private final NamedSql singularClaimLock, identifierLocks, identifierLocksGuarded;
    private final Map<Insertion, NamedSql> pushes = new EnumMap<>(Insertion.class);
    private final Map<Insertion, NamedSql> pushesWokenByCaller = new EnumMap<>(Insertion.class);
    private final Map<Ordering, NamedSql> polls = new EnumMap<>(Ordering.class);

    /** A store that migrates the schema with the built-in runner when an engine starts. */
    public PostgresTaskStore(DataSource dataSource) {
        this(dataSource, SchemaMode.MIGRATE);
    }

    public PostgresTaskStore(DataSource dataSource, SchemaMode mode) {
        this(dataSource, mode, TaskSchema.DEFAULT_SCHEMA);
    }

    /**
     * @param schema the schema's name, {@value TaskSchema#DEFAULT_SCHEMA} by default; see
     *               {@link TaskSchema#validName}. Applications that share a database each use their own
     * @throws IllegalArgumentException if the name isn't a plain lowercase identifier
     */
    public PostgresTaskStore(DataSource dataSource, SchemaMode mode, String schema) {
        this(dataSource, mode, schema, STUCK_TRANSACTION_TIMEOUT, NETWORK_TIMEOUT);
    }

    /**
     * How long the store's own transactions that hold locks others wait for may sit idle between
     * statements: a claim, which under a SINGULAR_* ordering holds its topic's lock until it
     * commits, a lease renewal, and a push of the store's own that locks identifiers (the
     * downstream work of a completion). A node that dies silently there (a frozen VM, a network
     * that drops packets without closing the connection) left its transaction open, and the locks
     * held, until the server's TCP keepalive gave up on it: hours, by default. Every node's claims
     * on that topic, or the other nodes' heartbeats, waited as long. Past this, Postgres ends the
     * session and its locks go with it.
     */
    public static final Duration STUCK_TRANSACTION_TIMEOUT = Duration.ofMinutes(1);

    /**
     * How long a statement of the store's own transactions may wait for the database's reply. A
     * reply lost on a connection the network dropped without closing never arrives, and without a
     * bound the thread waiting for it, a topic's poller or a completion, waited forever. Generous,
     * since a statement may rightly wait for a lock an application transaction holds.
     */
    public static final Duration NETWORK_TIMEOUT = Duration.ofMinutes(5);

    /** With other timeouts, for tests that don't want to wait minutes. */
    PostgresTaskStore(DataSource dataSource, SchemaMode mode, String schema, Duration stuckTransactionTimeout, Duration networkTimeout) {
        if (!stuckTransactionTimeout.isPositive() || !networkTimeout.isPositive()) {
            throw new IllegalArgumentException("Timeouts must be positive");
        }
        this.networkTimeout = networkTimeout;
        // Set by the first statement of the transactions it guards, so it costs no round trip of its own.
        String guard = "set_config('idle_in_transaction_session_timeout', '" + stuckTransactionTimeout.toMillis() + "', true)";
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.mode = Objects.requireNonNull(mode, "mode");
        this.schema = TaskSchema.validName(schema);
        // The SQL below is written for the default schema; sql() qualifies it with this one.
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
        // Task rows are found the same way whatever the topic's history: by "t.id = ANY (array)",
        // one primary key lookup per id, with no topic condition next to it. With one, a planner
        // without statistics walked task_completed_idx by topic and matched the ids in a filter,
        // reading the topic's whole history on every completion (NOTIFY benchmark). A history
        // lookup by identifier binds both columns of task_identifier_idx, and compares the outcome
        // as text so task_completed_idx (topic_id, outcome) can't look narrower. It takes the topic
        // from a subquery, so the planner estimates an average topic's share of the history: given
        // the value, statistics analyzed before the topic had history say it has none, and walking
        // task_completed_idx by topic alone looked as cheap as looking up each identifier. No statement
        // runs on a cached generic plan (NamedSql). CompletionPlanTest finishes tasks every way
        // next to 50,000 finished tasks of the same topic.
        createTopic = sql("SELECT pgtasks.create_topic(:name)");
        findTopic = sql("SELECT id FROM pgtasks.topic WHERE name = :name");
        listTopics = sql("SELECT id, name FROM pgtasks.topic ORDER BY id");
        // Both also read the database's time, which sets this store's clock (DatabaseClock).
        registerNode = sql("INSERT INTO pgtasks.node (name, version) VALUES (:name, :version) RETURNING id, clock_timestamp()");
        heartbeat = sql("UPDATE pgtasks.node SET heartbeat_at = now() WHERE id = :node RETURNING clock_timestamp()");
        // An APPEND push only inserts, so its generic plan can't go wrong, and it is the statement run
        // most often: a producer may push one task per transaction. The others look up older tasks.
        for (Insertion insertion : Insertion.values()) {
            NamedSql atCommit = sql(pushSql(insertion, WakeUp.AT_COMMIT)), byCaller = sql(pushSql(insertion, WakeUp.BY_CALLER));
            pushes.put(insertion, insertion == Insertion.APPEND ? atCommit.withGenericPlans() : atCommit);
            pushesWokenByCaller.put(insertion, insertion == Insertion.APPEND ? byCaller.withGenericPlans() : byCaller);
        }
        // Order per identifier is task-id order, and ids are drawn before commit. Two pushes for one
        // identifier could commit out of id order, and the one that commits first can't see the
        // other's task to supersede or replace it: an older task would then outlive, or run
        // after, a newer one. So a push that does more than append locks each of its identifiers,
        // in one order so two pushes can't deadlock on them, before its statement draws ids, and
        // holds the locks until it commits; the next push's statement takes its snapshot after
        // that. APPEND pushes take no lock (they don't touch earlier tasks, and are the hot path),
        // so concurrent APPEND pushes for one identifier may still commit out of id order.
        // These are single-bigint keys, a keyspace apart from the two-integer keys of claims;
        // topics share a hash only by chance, which merely serializes those pushes.
        identifierLocks = sql("""
            SELECT count(pg_advisory_xact_lock(key))
            FROM (SELECT DISTINCT hashtextextended(identifier, :topic::bigint) AS key FROM unnest(:identifiers::text[]) AS identifier ORDER BY 1) keys
            """);
        // In the store's own transactions only: an application's transaction is the application's.
        identifierLocksGuarded = sql("""
            SELECT count(pg_advisory_xact_lock(key)), max(guard)
            FROM (SELECT DISTINCT hashtextextended(identifier, :topic::bigint) AS key FROM unnest(:identifiers::text[]) AS identifier ORDER BY 1) keys
            CROSS JOIN (SELECT %s AS guard) AS guard
            """.formatted(guard));
        for (Ordering ordering : Ordering.values()) {
            polls.put(ordering, sql(pollSql(ordering)));
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
                   set_config('enable_mergejoin', 'off', true), %s
            """.formatted(guard));
        // A SINGULAR_* poll checks other tasks for each candidate's identifier in its statement
        // snapshot, where another node's uncommitted claim still looks ready: with an earlier task
        // turned ready meanwhile (a promotion, a release), both nodes would claim one identifier.
        // So SINGULAR_* claims on a topic take turns until commit, and each poll's snapshot shows
        // the previous claim. FIFO claims don't wait.
        singularClaimLock = sql("SELECT pg_advisory_xact_lock(" + SINGULAR_CLAIM_LOCK + ", :topic::integer)");
        // Fenced: only rows still active under this node with the claimed attempt match.
        finish = sql("""
            WITH d AS (
                SELECT * FROM unnest(:ids::bigint[], :attempts::integer[], :outcomes::text[], :outputs::text[])
                    AS d(task_id, attempt, outcome, output)
            ),
            done AS (
                DELETE FROM pgtasks.queue q
                USING d
                WHERE q.task_id = ANY (:ids::bigint[])
                  AND q.task_id = d.task_id
                  AND q.attempt = d.attempt
                  AND q.node_id = :node::integer
                  AND q.topic_id = :topic::integer
                  AND q.state = 'active'
                RETURNING q.task_id, q.attempt, d.outcome, d.output
            )
            UPDATE pgtasks.task t
            SET outcome = done.outcome::pgtasks.task_outcome,
                output = done.output,
                attempts = done.attempt,
                completed_at = now()
            FROM done
            WHERE t.id = ANY (ARRAY(SELECT task_id FROM done))
              AND t.id = done.task_id
            RETURNING t.id
            """);
        // Non-counted waits (suspended, later, uncounted failures) give the attempt back. That is safe for
        // fencing: only the current owner can do it, so a stale claim's attempt never comes back.
        wait = sql("""
            WITH d AS (
                SELECT * FROM unnest(:ids::bigint[], :attempts::integer[], :states::text[], :delays::bigint[],
                                     :uncounted::integer[], :errors::text[])
                    AS d(task_id, attempt, state, delay_ms, uncounted, error)
            ),
            moved AS (
                UPDATE pgtasks.queue q
                SET state = d.state::pgtasks.queue_state,
                    run_at = CASE WHEN d.delay_ms IS NULL THEN NULL ELSE now() + d.delay_ms * interval '1 millisecond' END,
                    node_id = NULL,
                    claimed_at = NULL,
                    attempt = q.attempt - d.uncounted,
                    -- A run that ends waiting by its own decision (later, suspended) didn't fail, so
                    -- the failures in a row end there; a release (no error, retrying) isn't a run's end.
                    failures = CASE WHEN d.error IS NOT NULL THEN q.failures + 1
                                    WHEN d.state IN ('scheduled', 'suspended') THEN 0 ELSE q.failures END,
                    first_failed_at = CASE WHEN d.error IS NOT NULL THEN coalesce(q.first_failed_at, now())
                                           WHEN d.state IN ('scheduled', 'suspended') THEN NULL ELSE q.first_failed_at END
                FROM d
                WHERE q.task_id = ANY (:ids::bigint[])
                  AND q.task_id = d.task_id
                  AND q.attempt = d.attempt
                  AND q.node_id = :node::integer
                  AND q.topic_id = :topic::integer
                  AND q.state = 'active'
                RETURNING q.task_id, q.state, q.attempt
            ),
            -- Every failure, counted or not, leaves its error on the task, so what is failing shows
            -- while it retries, not only once it is dead.
            last_error AS (
                UPDATE pgtasks.task t
                SET error = d.error::jsonb, attempts = m.attempt
                FROM moved m JOIN d ON d.task_id = m.task_id
                WHERE t.id = ANY (ARRAY(SELECT task_id FROM moved))
                  AND t.id = m.task_id AND d.error IS NOT NULL
            )
            SELECT task_id FROM moved
            """);
        // Only the task's error: the queue row, and the claim, stay as they are. Fenced like wait.
        noteError = sql("""
            WITH d AS (
                SELECT * FROM unnest(:ids::bigint[], :attempts::integer[]) AS d(task_id, attempt)
            ),
            held AS (
                SELECT q.task_id FROM pgtasks.queue q
                JOIN d ON d.task_id = q.task_id AND d.attempt = q.attempt
                WHERE q.task_id = ANY (:ids::bigint[])
                  AND q.node_id = :node::integer
                  AND q.topic_id = :topic::integer
                  AND q.state = 'active'
            )
            UPDATE pgtasks.task t
            SET error = :error::jsonb
            WHERE t.id = ANY (ARRAY(SELECT task_id FROM held))
            RETURNING t.id
            """);
        // A LIMIT ... FOR UPDATE SKIP LOCKED selection must be materialized: as the inner side of a
        // semi-join it can be rescanned, and each rescan skips the rows this statement already
        // changed and returns the next ones, so the limit stops holding. The time is read once in
        // a subquery: compared with clock_timestamp() directly, which is volatile, run_at can't
        // bound the index scan, and every promotion walked past every task not due yet (found by
        // the drained-queue pass of the TCK).
        promote = sql("""
            WITH due AS MATERIALIZED (
                SELECT o.ctid AS row FROM pgtasks.queue o
                WHERE o.state IN ('scheduled', 'retrying') AND o.run_at <= (SELECT clock_timestamp())
                ORDER BY o.run_at
                LIMIT :limit::integer
                FOR UPDATE OF o SKIP LOCKED
            )
            UPDATE pgtasks.queue q
            SET state = 'ready', run_at = NULL
            WHERE q.ctid = ANY (ARRAY(SELECT row FROM due))
            RETURNING q.topic_id
            """);
        // The attempt is kept: an expired run counts, since a poison task can take a node down.
        expire = sql("""
            WITH stale AS (
                SELECT o.ctid AS row
                FROM pgtasks.queue o
                WHERE o.state = 'active'
                  AND o.node_id <> :node::integer
                  AND o.node_id IN (SELECT id FROM pgtasks.node WHERE heartbeat_at < now() - :expiry::bigint * interval '1 millisecond')
                FOR UPDATE OF o SKIP LOCKED
            ),
            -- Each expiry backs off a little longer, and the deadLetterAt-th one dead-letters the task:
            -- a task that takes its node down can't move from node to node forever.
            requeued AS (
                UPDATE pgtasks.queue q
                SET state = CASE WHEN q.expiries + 1 >= :dead_at::integer THEN 'failed'
                                 WHEN :backoff::bigint = 0 THEN 'ready' ELSE 'retrying' END::pgtasks.queue_state,
                    run_at = CASE WHEN q.expiries + 1 >= :dead_at::integer OR :backoff::bigint = 0 THEN NULL
                                  ELSE now() + :backoff::bigint * power(2, least(q.expiries, 20)) * interval '1 millisecond' END,
                    node_id = NULL, claimed_at = NULL,
                    expiries = q.expiries + 1,
                    failures = q.failures + 1,
                    first_failed_at = coalesce(q.first_failed_at, now())
                WHERE q.ctid = ANY (ARRAY(SELECT row FROM stale))
                RETURNING q.task_id, q.topic_id, q.state, q.attempt, q.expiries
            ),
            last_error AS (
                UPDATE pgtasks.task t
                SET error = jsonb_build_object('message', CASE WHEN r.state = 'failed'
                        THEN 'Expired ' || r.expiries || ' times: the node running it stopped every time'
                        ELSE 'Expired: the node running it stopped' END, 'type', 'expired'),
                    attempts = r.attempt
                FROM requeued r
                WHERE t.id = ANY (ARRAY(SELECT task_id FROM requeued))
                  AND t.id = r.task_id
            )
            SELECT topic_id FROM requeued
            """);
        findTask = sql("""
            SELECT tp.name AS topic, t.id, t.identifier, coalesce(q.state::text, t.outcome::text) AS state, q.task_id IS NULL AS finished,
                   coalesce(q.attempt, t.attempts) AS attempt, q.run_at, coalesce(q.priority, 0) AS priority, q.node_id,
                   coalesce(q.failures, 0) AS failures, q.first_failed_at,
                   t.reference, t.parent_id, t.root_id, t.payload, t.payload_type, t.output, t.error::text AS error,
                   t.previous_outcome::text AS previous_outcome, t.created_at, t.completed_at, t.headers::text AS headers
            FROM pgtasks.task t
            JOIN pgtasks.topic tp ON tp.id = t.topic_id
            LEFT JOIN pgtasks.queue q ON q.task_id = t.id
            WHERE t.id = :id::bigint
            """);
        // Dead letters are few, so queue_failed_idx keeps these to the topic's dead letters.
        deadLetters = sql("""
            SELECT tp.name AS topic, t.id, t.identifier, q.state::text AS state, false AS finished,
                   q.attempt, q.run_at, q.priority, q.node_id, q.failures, q.first_failed_at,
                   t.reference, t.parent_id, t.root_id, t.payload, t.payload_type, t.output, t.error::text AS error,
                   t.previous_outcome::text AS previous_outcome, t.created_at, t.completed_at, t.headers::text AS headers
            FROM pgtasks.queue q
            JOIN pgtasks.task t ON t.id = q.task_id
            JOIN pgtasks.topic tp ON tp.id = q.topic_id
            WHERE q.topic_id = :topic::integer AND q.state = 'failed' AND q.task_id > :after::bigint
              AND (:all::boolean OR q.task_id = ANY (:ids::bigint[]))
              AND (:error::text IS NULL OR coalesce(t.error->>'type', t.error->>'message', 'unknown') = :error::text)
              AND (:from::timestamptz IS NULL OR q.first_failed_at >= :from::timestamptz)
              AND (:until::timestamptz IS NULL OR q.first_failed_at < :until::timestamptz)
            ORDER BY q.task_id
            LIMIT :limit::integer
            """);
        String pickDeadLetters = """
            WITH picked AS MATERIALIZED (
                SELECT o.ctid AS row FROM pgtasks.queue o
                JOIN pgtasks.task t ON t.id = o.task_id
                WHERE o.topic_id = :topic::integer AND o.state = 'failed' AND o.task_id > :after::bigint
                  AND (:all::boolean OR o.task_id = ANY (:ids::bigint[]))
                  AND (:error::text IS NULL OR coalesce(t.error->>'type', t.error->>'message', 'unknown') = :error::text)
                  AND (:from::timestamptz IS NULL OR o.first_failed_at >= :from::timestamptz)
                  AND (:until::timestamptz IS NULL OR o.first_failed_at < :until::timestamptz)
                ORDER BY o.task_id
                LIMIT :limit::integer
                FOR UPDATE OF o SKIP LOCKED
            )""";
        // The statement trigger on pgtasks.queue wakes the workers.
        revive = sql(pickDeadLetters + """
            UPDATE pgtasks.queue q
            SET state = 'ready', attempt = 0, run_at = NULL, failures = 0, first_failed_at = NULL, expiries = 0
            WHERE q.ctid = ANY (ARRAY(SELECT row FROM picked))
            RETURNING q.task_id
            """);
        discard = sql(pickDeadLetters + """
            , gone AS (
                DELETE FROM pgtasks.queue q
                WHERE q.ctid = ANY (ARRAY(SELECT row FROM picked))
                RETURNING q.task_id, q.attempt
            )
            UPDATE pgtasks.task t
            SET outcome = 'failed', attempts = gone.attempt, completed_at = now()
            FROM gone
            WHERE t.id = ANY (ARRAY(SELECT task_id FROM gone))
              AND t.id = gone.task_id
            RETURNING t.id
            """);
        // An operator's search of a topic's unfinished tasks, and the actions on what it finds. Each
        // condition is a parameter that is either null, which the plan made for these values folds
        // away, or a value. The candidates come from the queue alone, into an array of row
        // addresses: by the topic, in queue_identifier_idx (narrowed by the identifier if given), or
        // by the ids, or by the tasks with the reference (task_reference_idx). The task's conditions are checked per candidate by primary key, fenced with
        // OFFSET 0 as in the poll. The rows are then read by address (a TID scan), checked again as
        // locked, and sorted by id; the search reads their tasks only after the limit.
        //
        // While the queue's statistics say it is empty (DrainedQueue), every plan looks free, and
        // anything else let the planner walk every topic's tasks: a join of queue and task, the
        // queue's primary key in id order for a merge join; "task_id > :after" or "ORDER BY task_id",
        // the primary key from there or in order; the state as an enum rather than text,
        // queue_due_idx or queue_active_idx, which aren't by topic. Without an identifier or ids
        // this reads the topic's backlog, as counting it does, once per page or batch of 1,000; an
        // index on (topic_id, task_id) for this alone would slow every push and claim. A row that a concurrent commit moved is
        // skipped, which only delays acting on it, and a task claimed meanwhile fails the recheck.
        String candidates = """
            o.ctid = ANY (ARRAY(
                SELECT c.ctid FROM pgtasks.queue c
                WHERE c.topic_id = :topic::integer AND c.task_id + 0 > :after::bigint
                  AND (:all::boolean OR c.task_id = ANY (:ids::bigint[]))
                  AND (:identifier::text IS NULL OR c.identifier = :identifier::text)
                  AND (:reference::text IS NULL OR c.task_id = ANY (ARRAY(
                      SELECT r.id FROM pgtasks.task r WHERE r.reference = :reference::text)))
                  AND (:any_state::boolean OR c.state::text = ANY (:states::text[]))
                  AND (:any_task::boolean OR EXISTS (
                      SELECT 1 FROM pgtasks.task t
                      WHERE t.id = c.task_id
                        AND (:reference::text IS NULL OR t.reference = :reference::text)
                        AND (:header::text IS NULL OR t.headers @> jsonb_build_object(:header::text, :header_value::text))
                        AND (:from::timestamptz IS NULL OR t.created_at >= :from::timestamptz)
                        AND (:until::timestamptz IS NULL OR t.created_at < :until::timestamptz)
                      OFFSET 0))))
              AND o.task_id + 0 > :after::bigint
              AND (:any_state::boolean OR o.state::text = ANY (:states::text[]))
            """;
        searchTasks = sql("""
            WITH found AS MATERIALIZED (
                SELECT o.task_id, o.state, o.attempt, o.run_at, o.priority, o.node_id, o.failures, o.first_failed_at
                FROM pgtasks.queue o
                WHERE
            """ + candidates + """
                ORDER BY o.task_id + 0
                LIMIT :limit::integer
            )
            SELECT (SELECT name FROM pgtasks.topic WHERE id = :topic::integer) AS topic, t.id, t.identifier, o.state::text AS state,
                   false AS finished, o.attempt, o.run_at, o.priority, o.node_id, o.failures, o.first_failed_at,
                   t.reference, t.parent_id, t.root_id, t.payload, t.payload_type, t.output, t.error::text AS error,
                   t.previous_outcome::text AS previous_outcome, t.created_at, t.completed_at, t.headers::text AS headers
            FROM found o
            JOIN pgtasks.task t ON t.id = o.task_id
            ORDER BY o.task_id
            """);
        // The query's states are set to suspended alone (see releaseSuspended), so the candidates
        // and their recheck as locked take nothing else. The update trigger on pgtasks.queue wakes the workers.
        releaseSuspended = sql("""
            WITH picked AS MATERIALIZED (
                SELECT o.ctid AS row FROM pgtasks.queue o
                WHERE
            """ + candidates + """
                ORDER BY o.task_id + 0
                LIMIT :limit::integer
                FOR UPDATE OF o SKIP LOCKED
            )
            UPDATE pgtasks.queue q
            SET state = 'ready'
            WHERE q.ctid = ANY (ARRAY(SELECT row FROM picked))
            RETURNING q.task_id
            """);
        // A node's lease goes with it (ON DELETE CASCADE); one that still holds claims stays.
        deleteDeadNodes = sql("""
            DELETE FROM pgtasks.node n
            WHERE n.heartbeat_at < now() - :dead::bigint * interval '1 millisecond'
              -- Only active rows have a node (a CHECK says so), and queue_active_idx covers those.
              AND NOT EXISTS (SELECT 1 FROM pgtasks.queue q WHERE q.node_id = n.id AND q.state = 'active')
            """);
        deregisterNode = sql("""
            DELETE FROM pgtasks.node n
            WHERE n.id = :id::integer
              AND NOT EXISTS (SELECT 1 FROM pgtasks.queue q WHERE q.node_id = n.id AND q.state = 'active')
            """);
        listNodes = sql("""
            SELECT n.id, n.name, n.version, n.started_at, n.heartbeat_at,
                   coalesce((SELECT array_agg(l.name ORDER BY l.name) FROM pgtasks.lease l
                             WHERE l.node_id = n.id AND l.expires_at >= now()), '{}') AS leases
            FROM pgtasks.node n
            ORDER BY n.id
            """);
        // A discarded dead letter may have held back later tasks; no trigger sees a delete.
        notifyReady = sql("SELECT pg_notify('" + schema + "_ready', t::text) FROM unnest(:topics::integer[]) AS t");
        // The channel name is the schema's, so another schema's nodes don't hear it.
        notifyChannel = sql("SELECT pg_notify(:channel::text, :payload::text)");
        countQueue = sql("""
            SELECT count(*) FILTER (WHERE state = 'scheduled'),
                   count(*) FILTER (WHERE state = 'retrying'),
                   count(*) FILTER (WHERE state = 'ready'),
                   count(*) FILTER (WHERE state = 'active'),
                   count(*) FILTER (WHERE state = 'suspended'),
                   count(*) FILTER (WHERE state = 'failed'),
                   count(*) FILTER (WHERE state IN ('scheduled', 'retrying') AND run_at <= clock_timestamp()),
                   min(run_at),
                   (SELECT created_at FROM pgtasks.task WHERE id = min(task_id) FILTER (WHERE state = 'ready')) AS oldest_ready_at,
                   count(*) FILTER (WHERE failures > 0) AS failing,
                   min(first_failed_at) AS oldest_failing_at
            FROM pgtasks.queue
            WHERE topic_id = :topic::integer
            """);
        // Materialized for the same reason as promote: otherwise the limit doesn't hold. One range
        // of task_completed_idx (topic_id, outcome, completed_at).
        purge = sql("""
            WITH doomed AS MATERIALIZED (
                SELECT d.id FROM pgtasks.task d
                WHERE d.topic_id = :topic::integer AND d.outcome = :outcome::pgtasks.task_outcome AND d.completed_at < :before::timestamptz
                ORDER BY d.completed_at
                LIMIT :limit::integer
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM pgtasks.task t
            WHERE t.id = ANY (ARRAY(SELECT id FROM doomed))
            """);
        // One range of task_completed_idx too: every finished task of the topic. An unfinished one
        // stays, held by its queue row.
        purgeAll = sql("""
            WITH doomed AS MATERIALIZED (
                SELECT d.id FROM pgtasks.task d
                WHERE d.topic_id = :topic::integer AND d.outcome IS NOT NULL
                LIMIT :limit::integer
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM pgtasks.task t
            WHERE t.id = ANY (ARRAY(SELECT id FROM doomed))
            """);
        // A push's foreign key check takes a share lock on the topic row, so this waits for
        // pushes in flight and makes later ones wait, then fail, once the row is gone.
        lockTopic = sql("SELECT name, " + guard + " FROM pgtasks.topic WHERE id = :topic::integer FOR UPDATE");
        topicHasTasks = sql("SELECT EXISTS (SELECT 1 FROM pgtasks.task WHERE topic_id = :topic::integer)");
        // The delete trigger on pgtasks.topic tells every node (R__notify_topic.sql).
        deleteTopic = sql("""
            WITH counts AS (DELETE FROM pgtasks.topic_count WHERE topic_id = :topic::integer)
            DELETE FROM pgtasks.topic WHERE id = :topic::integer
            """);
        // Written at every start, so it skips the write when nothing changed.
        setRetention = sql("""
            UPDATE pgtasks.topic
            SET retention = jsonb_object(:outcomes::text[], :periods::text[])
            WHERE id = :topic::integer
              AND retention IS DISTINCT FROM jsonb_object(:outcomes::text[], :periods::text[])
            """);
        retention = sql("""
            SELECT t.id, r.key, r.value
            FROM pgtasks.topic t
            CROSS JOIN LATERAL jsonb_each_text(t.retention) AS r
            ORDER BY t.id
            """);
        snapshot = sql("""
            INSERT INTO pgtasks.topic_count (topic_id, taken_at, scheduled, retrying, ready, active, suspended, failed, due, next_run_at,
                                           oldest_ready_at, failing, oldest_failing_at)
            SELECT t.id, now(),
                   count(q.task_id) FILTER (WHERE q.state = 'scheduled'),
                   count(q.task_id) FILTER (WHERE q.state = 'retrying'),
                   count(q.task_id) FILTER (WHERE q.state = 'ready'),
                   count(q.task_id) FILTER (WHERE q.state = 'active'),
                   count(q.task_id) FILTER (WHERE q.state = 'suspended'),
                   count(q.task_id) FILTER (WHERE q.state = 'failed'),
                   count(q.task_id) FILTER (WHERE q.state IN ('scheduled', 'retrying') AND q.run_at <= now()),
                   min(q.run_at),
                   (SELECT created_at FROM pgtasks.task WHERE id = min(q.task_id) FILTER (WHERE q.state = 'ready')),
                   count(q.task_id) FILTER (WHERE q.failures > 0),
                   min(q.first_failed_at)
            FROM pgtasks.topic t
            LEFT JOIN pgtasks.queue q ON q.topic_id = t.id
            GROUP BY t.id
            ON CONFLICT (topic_id) DO UPDATE SET taken_at = excluded.taken_at, scheduled = excluded.scheduled, retrying = excluded.retrying,
                ready = excluded.ready, active = excluded.active, suspended = excluded.suspended, failed = excluded.failed,
                due = excluded.due, next_run_at = excluded.next_run_at, oldest_ready_at = excluded.oldest_ready_at,
                failing = excluded.failing, oldest_failing_at = excluded.oldest_failing_at
            """);
        // Takes a lease nobody holds or whose term ran out, or renews this node's own. Two nodes
        // asking at once serialize on the row; the second sees the first's fresh term and gets nothing.
        // Guarded: every node renews or asks for it on its heartbeat thread, so one stuck holding
        // the row would hold up every other node's heartbeat.
        lead = sql("""
            INSERT INTO pgtasks.lease AS l (name, node_id, acquired_at, expires_at)
            SELECT :name, :node::integer, now(), now() + :term::bigint * interval '1 millisecond'
            FROM (SELECT %s) AS guard
            ON CONFLICT (name) DO UPDATE
            SET node_id = excluded.node_id,
                acquired_at = CASE WHEN l.node_id = excluded.node_id THEN l.acquired_at ELSE excluded.acquired_at END,
                expires_at = excluded.expires_at
            WHERE l.node_id = excluded.node_id OR l.expires_at < now()
            RETURNING l.expires_at
            """.formatted(guard));
        resign = sql("DELETE FROM pgtasks.lease WHERE name = :name AND node_id = :node::integer");
        snapshots = sql("""
            SELECT tp.name, c.scheduled, c.retrying, c.ready, c.active, c.suspended, c.failed, c.due, c.next_run_at, c.taken_at,
                   c.oldest_ready_at, c.failing, c.oldest_failing_at
            FROM pgtasks.topic_count c
            JOIN pgtasks.topic tp ON tp.id = c.topic_id
            ORDER BY tp.name
            """);
        activeClaims = sql("""
            SELECT topic_id, task_id, attempt, claimed_at
            FROM pgtasks.queue
            WHERE node_id = :node::integer AND state = 'active'
            ORDER BY task_id
            """);
        // Claims are stamped with clock_timestamp(), so their age is measured by it too.
        settledClaims = sql("""
            SELECT topic_id, task_id, attempt, claimed_at
            FROM pgtasks.queue
            WHERE node_id = :node::integer AND state = 'active'
              AND claimed_at <= clock_timestamp() - :age::bigint * interval '1 millisecond'
            ORDER BY task_id
            """);
    }


    @Override
    public void prepare() throws SQLException {
        TaskSchema.prepare(dataSource, mode, schema);
        if (mode == SchemaMode.NONE) {
            // Does nothing, and the schema may not exist yet; the leader's count snapshots still check.
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            sampleClock(connection);
            analyzeTasksIfStatisticsSayEmpty(connection);
        }
    }

    /** Sets the clock to the database's time at once, for engines that push before their first heartbeat, or never start. */
    private void sampleClock(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT clock_timestamp()")) {
            long before = clock.nanos();
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                clock.sample(requiredInstant(resultSet, "clock_timestamp"), before, clock.nanos());
            }
        }
    }

    /**
     * Statistics that say the task table has pages but no rows (analyzed right after retention
     * emptied it) make every index path on it look free, and equally so: the planner then picks
     * by tie, and a claim, completion or lookup can read every task (the TCK's EMPTIED pass). No
     * statement rewrite helps a tie, so the statistics are repaired instead: at prepare and with
     * every count snapshot on the leader (prepare skips it in SchemaMode.NONE), an ANALYZE when they say empty and a row exists. Rarely
     * true, and false again once analyzed; the queue isn't touched, its statements are built for
     * "analyzed while empty". A role that doesn't own the table gets a warning from Postgres and
     * nothing else; an error is rolled back to before the check, so the caller's work stands.
     */
    private void analyzeTasksIfStatisticsSayEmpty(Connection connection) throws SQLException {
        boolean autoCommit = connection.getAutoCommit();
        Savepoint before = autoCommit ? null : connection.setSavepoint();
        try {
            boolean empty;
            try (PreparedStatement statement = connection.prepareStatement(TaskSchema.qualify(TASK_STATISTICS_SAY_EMPTY, schema));
                 ResultSet resultSet = statement.executeQuery()) {
                empty = resultSet.next() && resultSet.getBoolean(1);
            }
            if (empty) {
                try (PreparedStatement statement = connection.prepareStatement(TaskSchema.qualify("ANALYZE pgtasks.task", schema))) {
                    statement.execute();
                }
            }
            if (before != null) {
                connection.releaseSavepoint(before);
            }
        } catch (SQLException e) {
            if (before == null) {
                throw e;
            }
            connection.rollback(before);
        }
    }

    /** The CASE keeps the table from being read unless its statistics say it is empty. */
    private static final String TASK_STATISTICS_SAY_EMPTY = """
        SELECT CASE WHEN c.reltuples = 0 AND c.relpages > 0 THEN EXISTS (SELECT 1 FROM pgtasks.task) ELSE false END
        FROM pg_class c
        WHERE c.oid = 'pgtasks.task'::regclass
        """;

    /**
     * The database's time, read at each heartbeat and node registration and moved on by the JVM's
     * monotonic clock in between, so the engine's comparisons with times the database stamped agree
     * with the database even when this node's wall clock is off. See {@link DatabaseClock}.
     */
    @Override
    public InstantSource clock() {
        return clock;
    }

    /** The schema this store uses, for extensions that keep their own tables beside its own. */
    public String schema() {
        return schema;
    }

    /** How this store treats its schema, so an extension knows whether it may create its own tables. */
    public SchemaMode mode() {
        return mode;
    }

    /** The store's DataSource, for extensions that apply their own SQL files to the same database. */
    public DataSource dataSource() {
        return dataSource;
    }

    @Override
    public StoreTransaction begin() throws SQLException {
        return PostgresTransaction.open(dataSource.getConnection(), networkTimeout);
    }

    @Override
    public boolean joinsApplicationTransactions() {
        return true;
    }

    @Override
    public StoreTransaction join(Connection connection) {
        return PostgresTransaction.join(connection);
    }

    private NamedSql sql(String text) {
        return NamedSql.parse(TaskSchema.qualify(text, schema));
    }

    private static String pushSql(Insertion insertion, WakeUp wakeUp) {
        // One statement: ids are drawn in input order, queue rows are inserted first so a dedup
        // conflict also skips the task row, then older rows are handled for the identifiers
        // that were actually queued. Every CTE sees the snapshot from before the statement, so
        // none of them can touch the rows this statement inserts.
        //
        // Older queue rows are deleted by task_id, and also by topic and identifier, which never
        // change: by task_id alone, a planner that believes the queue is empty may look them up in
        // queue_identifier_idx, where task_id is the last column, and walk the whole index on every
        // push (found by the drained-queue pass of the TCK). REPLACE marks earlier finished tasks
        // by one array of ids, so it reaches the task table by primary key, as every statement does.
        String cleanup = switch (insertion) {
            case APPEND -> "";
            case SUPERSEDE -> """
                ,
                superseded AS (
                    DELETE FROM pgtasks.queue q
                    WHERE q.topic_id = :topic::integer
                      AND q.identifier = ANY (ARRAY(SELECT identifier FROM queued))
                      AND q.task_id = ANY (ARRAY(
                        SELECT o.task_id FROM pgtasks.queue o
                        WHERE o.topic_id = :topic::integer
                          AND o.identifier = ANY (ARRAY(SELECT identifier FROM queued))
                          AND o.state IN ('ready', 'scheduled', 'retrying')
                        FOR UPDATE OF o SKIP LOCKED))
                    RETURNING q.task_id
                ),
                superseded_tasks AS (
                    UPDATE pgtasks.task t SET outcome = 'redundant', completed_at = statement_timestamp()
                    WHERE t.id = ANY (ARRAY(SELECT task_id FROM superseded))
                )
                """;
            case REPLACE -> """
                ,
                replaced AS (
                    DELETE FROM pgtasks.queue q
                    WHERE q.topic_id = :topic::integer
                      AND q.identifier = ANY (ARRAY(SELECT identifier FROM queued))
                      AND q.task_id = ANY (ARRAY(
                        SELECT o.task_id FROM pgtasks.queue o
                        WHERE o.topic_id = :topic::integer
                          AND o.identifier = ANY (ARRAY(SELECT identifier FROM queued))
                          AND o.state <> 'active'
                        FOR UPDATE OF o SKIP LOCKED))
                    RETURNING q.task_id
                ),
                replaced_open AS (
                    UPDATE pgtasks.task t SET outcome = 'redundant', completed_at = statement_timestamp()
                    WHERE t.id = ANY (ARRAY(SELECT task_id FROM replaced))
                ),
                replaced_done AS (
                    UPDATE pgtasks.task t SET previous_outcome = t.outcome, outcome = 'redundant'
                    WHERE t.id = ANY (ARRAY(
                        SELECT d.id FROM pgtasks.task d
                        WHERE d.topic_id = (SELECT :topic::integer)
                          AND d.identifier = ANY (ARRAY(SELECT identifier FROM queued))
                          AND d.outcome::text IN ('succeeded', 'filtered', 'failed')
                        FOR UPDATE OF d SKIP LOCKED))
                )
                """;
        };
        // Woken by the caller: the flag tells the queue's insert trigger, which runs at the end of
        // this statement, to skip its notification; the trigger clears it, so it covers this
        // statement only (R__notify_ready.sql).
        String quiet = switch (wakeUp) {
            case AT_COMMIT -> "";
            case BY_CALLER -> "\n    CROSS JOIN (SELECT set_config('pgtasks.notify_after_commit', 'on', true)) AS quiet";
        };
        return """
            WITH input AS MATERIALIZED (
                SELECT nextval('pgtasks.task_id_seq') AS id, i.*
                FROM unnest(:identifiers::text[], :payloads::text[], :types::text[], :references::text[],
                            :parents::bigint[], :roots::bigint[], :states::text[], :delays::bigint[],
                            :priorities::integer[], :dedup::text[], :headers::text[], :run_ats::text[])
                    WITH ORDINALITY AS i(identifier, payload, payload_type, reference, parent_id, root_id,
                                         state, delay_ms, priority, dedup_key, headers, run_at, ord)""" + quiet + """

            ),
            -- A delayed task waits for its run time: given, or the delay from now. One that has come,
            -- by the clock of the database, is ready at once.
            queued AS (
                INSERT INTO pgtasks.queue (task_id, topic_id, identifier, state, priority, run_at, dedup_key)
                SELECT id, :topic::integer, identifier,
                       CASE WHEN state = 'scheduled' AND due <= statement_timestamp() THEN 'ready' ELSE state END::pgtasks.queue_state, priority,
                       CASE WHEN state = 'scheduled' AND due > statement_timestamp() THEN due END,
                       dedup_key
                FROM input
                CROSS JOIN LATERAL (SELECT coalesce(run_at::timestamptz, statement_timestamp() + delay_ms * interval '1 millisecond') AS due) AS d
                WHERE state IS NOT NULL
                ORDER BY ord
                ON CONFLICT (topic_id, dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING
                RETURNING task_id, identifier
            ),
            stored AS (
                INSERT INTO pgtasks.task (topic_id, id, identifier, reference, payload, payload_type, parent_id, root_id, headers,
                                        created_at, outcome, completed_at)
                SELECT :topic::integer, id, identifier, reference, payload, payload_type, parent_id, root_id, headers::jsonb,
                       statement_timestamp(),
                       CASE WHEN state IS NULL THEN 'redundant'::pgtasks.task_outcome END,
                       CASE WHEN state IS NULL THEN statement_timestamp() END
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
                      SELECT 1 FROM pgtasks.queue b
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
                FROM pgtasks.queue c
                WHERE c.topic_id = :topic::integer
                  AND c.state = 'ready'
            """ + condition + """
                ORDER BY c.priority DESC, c.task_id
                LIMIT :limit::integer
                FOR UPDATE OF c SKIP LOCKED
            ),
            claimed AS (
                UPDATE pgtasks.queue q
                SET state = 'active', node_id = :node::integer, attempt = q.attempt + 1, claimed_at = clock_timestamp()
                WHERE q.ctid = ANY (ARRAY(SELECT row FROM picked))
                RETURNING q.task_id, q.identifier, q.attempt, q.priority, q.claimed_at, q.failures, q.first_failed_at
            )
            SELECT c.task_id, c.identifier, c.attempt, c.priority, c.claimed_at, c.failures, c.first_failed_at,
                   t.payload, t.payload_type, t.reference, t.parent_id, t.root_id, t.created_at, t.headers::text AS headers
            FROM claimed c
            JOIN pgtasks.task t ON t.id = c.task_id
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
                topics.add(new TopicInfo(resultSet.getInt(1), resultSet.getString(2)));
            }
        }
        return topics;
    }

    @Override
    public List<Pushed> push(StoreTransaction transaction, int topicId, Insertion insertion, List<NewTask> tasks) throws SQLException {
        return push(transaction, topicId, insertion, tasks, WakeUp.AT_COMMIT);
    }

    /** {@link WakeUp#BY_CALLER} leaves the notification to {@link #notifyReady}, so the push's commit doesn't queue for the notify lock. */
    @Override
    public List<Pushed> push(StoreTransaction transaction, int topicId, Insertion insertion, List<NewTask> tasks, WakeUp wakeUp) throws SQLException {
        Connection connection = transaction.connection();
        if (tasks.isEmpty()) {
            return List.of();
        }
        // Within one push, SUPERSEDE and REPLACE keep earlier duplicates as redundant history.
        Map<String, Integer> last = new HashMap<>();
        for (int index = 0; index < tasks.size(); index++) {
            last.put(tasks.get(index).identifier(), index);
        }
        List<NewTask> stored = new ArrayList<>(tasks.size());
        List<String> states = new ArrayList<>(tasks.size());
        for (int index = 0; index < tasks.size(); index++) {
            NewTask task = tasks.get(index);
            boolean superseded = insertion != Insertion.APPEND && last.getOrDefault(task.identifier(), index) != index;
            stored.add(task);
            states.add(superseded ? null : task.suspended() ? "suspended" : task.delayed() ? "scheduled" : "ready");
        }
        int size = stored.size();
        @Nullable Object[] identifiers = new @Nullable Object[size], payloads = new @Nullable Object[size], types = new @Nullable Object[size], references = new @Nullable Object[size];
        @Nullable Object[] parents = new @Nullable Object[size], roots = new @Nullable Object[size], delays = new @Nullable Object[size], priorities = new @Nullable Object[size], dedup = new @Nullable Object[size];
        @Nullable Object[] headers = new @Nullable Object[size], runAts = new @Nullable Object[size];
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
            headers[index] = HeaderJson.write(task.headers());
            runAts[index] = task.runAt() == null ? null : task.runAt().toString();
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
            .array("headers", "text", headers)
            .array("run_ats", "text", runAts);
        if (insertion != Insertion.APPEND) {
            NamedSql locks = transaction instanceof PostgresTransaction own && own.owned() ? identifierLocksGuarded : identifierLocks;
            try (PreparedStatement statement = locks.prepare(connection, NamedSql.parameters().set("topic", topicId).array("identifiers", "text", identifiers));
                 ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
            }
        }
        List<Pushed> results = new ArrayList<>(tasks.size());
        try (PreparedStatement statement = Objects.requireNonNull((wakeUp == WakeUp.BY_CALLER ? pushesWokenByCaller : pushes).get(insertion)).prepare(connection, parameters);
             ResultSet resultSet = statement.executeQuery()) {
            for (int index = 0; index < tasks.size(); index++) {
                String identifier = tasks.get(index).identifier();
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
        if (request.ordering() != Ordering.FIFO) {
            try (PreparedStatement statement = singularClaimLock.prepare(connection, NamedSql.parameters().set("topic", request.topicId()))) {
                statement.execute();
            }
        }
        List<ClaimedTask> tasks = new ArrayList<>();
        NamedSql.Parameters parameters = NamedSql.parameters()
            .set("topic", request.topicId())
            .set("node", request.nodeId())
            .set("limit", request.limit());
        try (PreparedStatement statement = Objects.requireNonNull(polls.get(request.ordering())).prepare(connection, parameters);
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                long taskId = resultSet.getLong("task_id");
                tasks.add(new ClaimedTask(
                    taskId,
                    resultSet.getString("identifier"),
                    resultSet.getInt("attempt"),
                    resultSet.getInt("priority"),
                    resultSet.getString("payload"),
                    resultSet.getString("payload_type"),
                    resultSet.getString("reference"),
                    resultSet.getObject("parent_id", Long.class),
                    resultSet.getObject("root_id", Long.class),
                    requiredInstant(resultSet, "created_at"),
                    requiredInstant(resultSet, "claimed_at"),
                    HeaderJson.readOrNone(taskId, resultSet.getString("headers")),
                    resultSet.getInt("failures"),
                    instant(resultSet, "first_failed_at")
                ));
            }
        }
        return tasks.isEmpty() ? Claim.EMPTY : new Claim(List.copyOf(tasks));
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

    @Override
    public int noteError(StoreTransaction transaction, int topicId, int nodeId, List<ActiveClaim> claims, String error) throws SQLException {
        if (claims.isEmpty()) {
            return 0;
        }
        NamedSql.Parameters parameters = NamedSql.parameters()
            .set("topic", topicId)
            .set("node", nodeId)
            .array("ids", "bigint", claims.stream().map(ActiveClaim::taskId).toArray())
            .array("attempts", "integer", claims.stream().map(ActiveClaim::attempt).toArray())
            .set("error", storable(error));
        Set<Long> marked = new HashSet<>();
        collect(noteError, transaction.connection(), parameters, marked);
        return marked.size();
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
            @Nullable Object[] ids = new @Nullable Object[size], attempts = new @Nullable Object[size], outcomes = new @Nullable Object[size], outputs = new @Nullable Object[size];
            for (int index = 0; index < size; index++) {
                Completion completion = finished.get(index);
                Resolution.Finished resolution = (Resolution.Finished) completion.resolution();
                ids[index] = completion.taskId();
                attempts[index] = completion.attempt();
                outcomes[index] = switch (resolution.outcome()) {
                    case SUCCEEDED -> "succeeded";
                    case FILTERED -> "filtered";
                };
                outputs[index] = storable(resolution.output());
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
            @Nullable Object[] ids = new @Nullable Object[size], attempts = new @Nullable Object[size], states = new @Nullable Object[size], delays = new @Nullable Object[size];
            @Nullable Object[] uncounted = new @Nullable Object[size], errors = new @Nullable Object[size];
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
                errors[index] = storable(resolution.error());
            }
            NamedSql.Parameters parameters = NamedSql.parameters()
                .set("topic", topicId)
                .set("node", nodeId)
                .array("ids", "bigint", ids)
                .array("attempts", "integer", attempts)
                .array("states", "text", states)
                .array("delays", "bigint", delays)
                .array("uncounted", "integer", uncounted)
                .array("errors", "text", errors);
            collect(wait, connection, parameters, matched);
        }
        return matched;
    }

    /**
     * A task's output, filter reason or error as Postgres can store it: text and jsonb can't hold
     * a NUL character, and one in a single task's message would fail the completion of its whole
     * batch. It becomes U+FFFD; the text is for people, so nothing reads it back as data.
     */
    private static @Nullable String storable(@Nullable String text) {
        return text == null || text.indexOf('\0') < 0 ? text : text.replace('\0', '\uFFFD');
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
    public Promoted promoteDue(StoreTransaction transaction, int limit) throws SQLException {
        int tasks = 0;
        Set<Integer> topics = new LinkedHashSet<>();
        try (PreparedStatement statement = promote.prepare(transaction.connection(), NamedSql.parameters().set("limit", limit));
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                tasks++;
                topics.add(resultSet.getInt(1));
            }
        }
        return new Promoted(tasks, List.copyOf(topics));
    }

    @Override
    public List<Integer> requeueExpired(StoreTransaction transaction, int nodeId, Duration expiry, Expiries expiries) throws SQLException {
        Connection connection = transaction.connection();
        return topics(expire, connection, NamedSql.parameters().set("node", nodeId).set("expiry", expiry.toMillis())
            .set("dead_at", expiries.deadLetterAt()).set("backoff", expiries.backoff().toMillis()));
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
    public int purge(StoreTransaction transaction, int topicId, io.github.khopland.pgtasks.Outcome outcome, Instant completedBefore, int limit) throws SQLException {
        NamedSql.Parameters parameters = NamedSql.parameters()
            .set("topic", topicId)
            .set("outcome", outcome.label())
            .set("before", OffsetDateTime.ofInstant(completedBefore, ZoneOffset.UTC))
            .set("limit", limit);
        try (PreparedStatement statement = purge.prepare(transaction.connection(), parameters)) {
            return statement.executeUpdate();
        }
    }

    @Override
    public int purgeAll(StoreTransaction transaction, int topicId, int limit) throws SQLException {
        try (PreparedStatement statement = purgeAll.prepare(transaction.connection(), NamedSql.parameters().set("topic", topicId).set("limit", limit))) {
            return statement.executeUpdate();
        }
    }

    @Override
    public boolean deleteTopic(StoreTransaction transaction, int topicId) throws SQLException {
        Connection connection = transaction.connection();
        NamedSql.Parameters topic = NamedSql.parameters().set("topic", topicId);
        try (PreparedStatement lock = lockTopic.prepare(connection, topic); ResultSet locked = lock.executeQuery()) {
            if (!locked.next()) {
                return false;
            }
        }
        try (PreparedStatement check = topicHasTasks.prepare(connection, topic); ResultSet tasks = check.executeQuery()) {
            tasks.next();
            if (tasks.getBoolean(1)) {
                return false;
            }
        }
        try (PreparedStatement delete = deleteTopic.prepare(connection, topic)) {
            return delete.executeUpdate() == 1;
        }
    }

    @Override
    public void setRetention(StoreTransaction transaction, int topicId, Map<io.github.khopland.pgtasks.Outcome, Period> retention) throws SQLException {
        List<io.github.khopland.pgtasks.Outcome> outcomes = retention.keySet().stream().sorted().toList();
        NamedSql.Parameters parameters = NamedSql.parameters()
            .set("topic", topicId)
            .array("outcomes", "text", outcomes.stream().map(io.github.khopland.pgtasks.Outcome::label).toArray())
            .array("periods", "text", outcomes.stream().map(outcome -> String.valueOf(retention.get(outcome))).toArray());
        try (PreparedStatement statement = setRetention.prepare(transaction.connection(), parameters)) {
            statement.executeUpdate();
        }
    }

    @Override
    public Map<Integer, Map<io.github.khopland.pgtasks.Outcome, Period>> retention(StoreTransaction transaction) throws SQLException {
        Map<Integer, Map<io.github.khopland.pgtasks.Outcome, Period>> result = new LinkedHashMap<>();
        try (PreparedStatement statement = retention.prepare(transaction.connection(), NamedSql.parameters());
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                // Skips outcomes this version doesn't know, set by a newer node during a rolling deploy.
                String label = resultSet.getString(2);
                Optional<io.github.khopland.pgtasks.Outcome> outcome = Arrays.stream(io.github.khopland.pgtasks.Outcome.values())
                    .filter(candidate -> candidate.label().equals(label))
                    .findFirst();
                if (outcome.isPresent()) {
                    result.computeIfAbsent(resultSet.getInt(1), ignored -> new EnumMap<>(io.github.khopland.pgtasks.Outcome.class))
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
        analyzeTasksIfStatisticsSayEmpty(transaction.connection());
    }

    @Override
    public List<TopicSnapshot> snapshots(StoreTransaction transaction) throws SQLException {
        List<TopicSnapshot> result = new ArrayList<>();
        try (PreparedStatement statement = snapshots.prepare(transaction.connection(), NamedSql.parameters());
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                result.add(new TopicSnapshot(resultSet.getString(1), new QueueCounts(resultSet.getLong(2), resultSet.getLong(3), resultSet.getLong(4),
                    resultSet.getLong(5), resultSet.getLong(6), resultSet.getLong(7), resultSet.getLong(8), instant(resultSet, "next_run_at"),
                    instant(resultSet, "oldest_ready_at"), resultSet.getLong("failing"), instant(resultSet, "oldest_failing_at")),
                    requiredInstant(resultSet, "taken_at")));
            }
        }
        return result;
    }

    @Override
    public List<ActiveClaim> activeClaims(StoreTransaction transaction, int nodeId) throws SQLException {
        return activeClaims(activeClaims.prepare(transaction.connection(), NamedSql.parameters().set("node", nodeId)));
    }

    @Override
    public List<ActiveClaim> activeClaims(StoreTransaction transaction, int nodeId, Duration claimedFor) throws SQLException {
        if (claimedFor.isZero()) {
            return activeClaims(transaction, nodeId);
        }
        return activeClaims(settledClaims.prepare(transaction.connection(), NamedSql.parameters().set("node", nodeId).set("age", claimedFor.toMillis())));
    }

    private static List<ActiveClaim> activeClaims(PreparedStatement prepared) throws SQLException {
        List<ActiveClaim> claims = new ArrayList<>();
        try (PreparedStatement statement = prepared;
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                claims.add(new ActiveClaim(resultSet.getInt(1), resultSet.getLong(2), resultSet.getInt(3), requiredInstant(resultSet, "claimed_at")));
            }
        }
        return claims;
    }

    @Override
    public int registerNode(StoreTransaction transaction, String name, @Nullable String version) throws SQLException {
        Connection connection = transaction.connection();
        try (PreparedStatement statement = registerNode.prepare(connection, NamedSql.parameters().set("name", name).set("version", version))) {
            long before = clock.nanos();
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                clock.sample(requiredInstant(resultSet, "clock_timestamp"), before, clock.nanos());
                return resultSet.getInt(1);
            }
        }
    }

    @Override
    public boolean heartbeat(StoreTransaction transaction, int nodeId) throws SQLException {
        Connection connection = transaction.connection();
        try (PreparedStatement statement = heartbeat.prepare(connection, NamedSql.parameters().set("node", nodeId))) {
            long before = clock.nanos();
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return false;
                }
                clock.sample(requiredInstant(resultSet, "clock_timestamp"), before, clock.nanos());
                return true;
            }
        }
    }

    @Override
    public Optional<TaskView> find(StoreTransaction transaction, long taskId) throws SQLException {
        try (PreparedStatement statement = findTask.prepare(transaction.connection(), NamedSql.parameters().set("id", taskId));
             ResultSet resultSet = statement.executeQuery()) {
            return resultSet.next() ? Optional.of(taskView(resultSet)) : Optional.empty();
        }
    }

    @Override
    public List<TaskView> deadLetters(StoreTransaction transaction, int topicId, DeadLetterQuery query, int limit) throws SQLException {
        List<TaskView> result = new ArrayList<>();
        try (PreparedStatement statement = deadLetters.prepare(transaction.connection(), query(query, topicId, limit));
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                result.add(taskView(resultSet));
            }
        }
        return result;
    }

    @Override
    public List<Long> revive(StoreTransaction transaction, int topicId, DeadLetterQuery query, int limit) throws SQLException {
        return deadLetterIds(revive, transaction, topicId, query, limit);
    }

    @Override
    public List<Long> discard(StoreTransaction transaction, int topicId, DeadLetterQuery query, int limit) throws SQLException {
        List<Long> discarded = deadLetterIds(discard, transaction, topicId, query, limit);
        if (!discarded.isEmpty()) {
            notifyReady(transaction, List.of(topicId));
        }
        return discarded;
    }

    @Override
    public List<TaskView> tasks(StoreTransaction transaction, int topicId, TaskQuery query, int limit) throws SQLException {
        List<TaskView> result = new ArrayList<>();
        try (PreparedStatement statement = searchTasks.prepare(transaction.connection(), query(query, topicId, limit));
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                result.add(taskView(resultSet));
            }
        }
        return result;
    }

    @Override
    public List<Long> releaseSuspended(StoreTransaction transaction, int topicId, TaskQuery query, int limit) throws SQLException {
        return ids(releaseSuspended, transaction, query(query.inStates(QueueState.SUSPENDED), topicId, limit));
    }

    @Override
    public void notifyReady(StoreTransaction transaction, Collection<Integer> topicIds) throws SQLException {
        if (topicIds.isEmpty()) {
            return;
        }
        try (PreparedStatement statement = notifyReady.prepare(transaction.connection(),
            NamedSql.parameters().array("topics", "integer", topicIds.toArray()))) {
            statement.execute();
        }
    }

    @Override
    public void notify(StoreTransaction transaction, String channel, String payload) throws SQLException {
        if (!Notifier.CHANNEL.matcher(channel).matches() || channel.equals("ready")) {
            throw new IllegalArgumentException("Invalid channel '" + channel + "': a lowercase word other than ready");
        }
        try (PreparedStatement statement = notifyChannel.prepare(transaction.connection(),
            NamedSql.parameters().set("channel", TaskSchema.channel(schema, channel)).set("payload", payload))) {
            statement.execute();
        }
    }

    private static NamedSql.Parameters query(DeadLetterQuery query, int topicId, int limit) {
        return NamedSql.parameters()
            .set("topic", topicId)
            .set("after", query.afterId())
            .set("all", query.taskIds() == null)
            .array("ids", "bigint", query.taskIds() == null ? new Object[0] : query.taskIds().toArray())
            .set("error", query.error())
            .set("from", query.failedFrom() == null ? null : OffsetDateTime.ofInstant(query.failedFrom(), ZoneOffset.UTC))
            .set("until", query.failedUntil() == null ? null : OffsetDateTime.ofInstant(query.failedUntil(), ZoneOffset.UTC))
            .set("limit", query.taskIds() == null ? limit : Math.min(limit, query.taskIds().size()));
    }

    private static NamedSql.Parameters query(TaskQuery query, int topicId, int limit) {
        return NamedSql.parameters()
            .set("topic", topicId)
            .set("after", query.afterId())
            .set("all", query.taskIds() == null)
            .array("ids", "bigint", query.taskIds() == null ? new Object[0] : query.taskIds().toArray())
            .set("identifier", query.identifier())
            .set("any_task", query.reference() == null && query.headerName() == null && query.createdFrom() == null && query.createdUntil() == null)
            .set("reference", query.reference())
            .set("any_state", query.states().isEmpty())
            .array("states", "text", query.states().stream().map(QueueState::label).toArray())
            .set("header", query.headerName())
            .set("header_value", query.headerValue())
            .set("from", query.createdFrom() == null ? null : OffsetDateTime.ofInstant(query.createdFrom(), ZoneOffset.UTC))
            .set("until", query.createdUntil() == null ? null : OffsetDateTime.ofInstant(query.createdUntil(), ZoneOffset.UTC))
            .set("limit", query.taskIds() == null ? limit : Math.min(limit, query.taskIds().size()));
    }

    private static List<Long> deadLetterIds(NamedSql sql, StoreTransaction transaction, int topicId, DeadLetterQuery query, int limit) throws SQLException {
        return ids(sql, transaction, query(query, topicId, limit));
    }

    /** Runs a statement that returns task ids, and sorts them. */
    private static List<Long> ids(NamedSql sql, StoreTransaction transaction, NamedSql.Parameters parameters) throws SQLException {
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

    @Override
    public int deleteDeadNodes(StoreTransaction transaction, Duration deadFor) throws SQLException {
        try (PreparedStatement statement = deleteDeadNodes.prepare(transaction.connection(), NamedSql.parameters().set("dead", deadFor.toMillis()))) {
            return statement.executeUpdate();
        }
    }

    @Override
    public boolean deregisterNode(StoreTransaction transaction, int nodeId) throws SQLException {
        try (PreparedStatement statement = deregisterNode.prepare(transaction.connection(), NamedSql.parameters().set("id", nodeId))) {
            return statement.executeUpdate() > 0;
        }
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
            requiredInstant(resultSet, "created_at"),
            instant(resultSet, "completed_at"),
            resultSet.getInt("failures"),
            instant(resultSet, "first_failed_at"),
            HeaderJson.readOrNone(resultSet.getLong("id"), resultSet.getString("headers"))
        );
    }

    @Override
    public QueueCounts count(StoreTransaction transaction, int topicId) throws SQLException {
        try (PreparedStatement statement = countQueue.prepare(transaction.connection(), NamedSql.parameters().set("topic", topicId));
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return new QueueCounts(resultSet.getLong(1), resultSet.getLong(2), resultSet.getLong(3), resultSet.getLong(4),
                resultSet.getLong(5), resultSet.getLong(6), resultSet.getLong(7), instant(resultSet, "min"), instant(resultSet, "oldest_ready_at"),
                resultSet.getLong("failing"), instant(resultSet, "oldest_failing_at"));
        }
    }

    @Override
    public List<NodeView> nodes(StoreTransaction transaction) throws SQLException {
        List<NodeView> nodes = new ArrayList<>();
        try (PreparedStatement statement = listNodes.prepare(transaction.connection(), NamedSql.parameters());
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                nodes.add(new NodeView(resultSet.getInt("id"), resultSet.getString("name"), resultSet.getString("version"),
                    requiredInstant(resultSet, "started_at"), requiredInstant(resultSet, "heartbeat_at"),
                    List.of((String[]) resultSet.getArray("leases").getArray())));
            }
        }
        return nodes;
    }

    /** A timestamp of a NOT NULL column. */
    private static Instant requiredInstant(ResultSet resultSet, String column) throws SQLException {
        return Objects.requireNonNull(instant(resultSet, column), column);
    }

    private static @Nullable Instant instant(ResultSet resultSet, String column) throws SQLException {
        OffsetDateTime value = resultSet.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    /** LISTEN/NOTIFY on a connection outside the pool when the pool says how; see {@link NotifierConnections}. */
    @Override
    public Notifier notifier() {
        return new PostgresNotifier(NotifierConnections.of(dataSource), schema);
    }

    @Override
    public String toString() {
        return "PostgresTaskStore[" + dataSource + ", " + mode + ", " + schema + "]";
    }
}
