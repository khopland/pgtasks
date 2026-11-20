package org.example.task2.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Pattern;

import org.example.task2.Insertion;
import org.example.task2.Ordering;
import org.example.task2.Pushed;
import org.example.task2.spi.StaleClaimException;
import org.example.task2.spi.TaskStore;

/**
 * The task store on the {@code tasks} schema (see {@link TaskSchema}). Every operation is one
 * statement, except a claim under a strict rate limit, which also reads and charges the
 * limit's token bucket in the same transaction.
 */
public final class PostgresTaskStore implements TaskStore {

    private static final Pattern SCHEMA = Pattern.compile("[a-z_][a-z0-9_]{0,62}");

    private final String schema;
    private final NamedSql createTopic, findTopic, registerNode, heartbeat, ensureAdmission, admissionLock, admissionCharge;
    private final NamedSql finish, wait, promote, expire;
    private final Map<Insertion, NamedSql> pushes = new EnumMap<>(Insertion.class);
    private final Map<Ordering, NamedSql> polls = new EnumMap<>(Ordering.class);

    public PostgresTaskStore() {
        this(TaskSchema.DEFAULT_SCHEMA);
    }

    public PostgresTaskStore(String schema) {
        if (!SCHEMA.matcher(schema).matches()) {
            throw new IllegalArgumentException("Invalid schema name: " + schema);
        }
        this.schema = schema;
        createTopic = sql("SELECT tasks.create_topic(:name)");
        findTopic = sql("SELECT id FROM tasks.topic WHERE name = :name");
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
            polls.put(ordering, sql(pollSql(ordering)));
        }
        // Fenced: only rows still active under this node with the claimed attempt match.
        finish = sql("""
            WITH d AS (
                SELECT * FROM unnest(:ids::bigint[], :attempts::integer[], :outcomes::text[], :outputs::text[])
                    AS d(task_id, attempt, outcome, output)
            ),
            done AS (
                DELETE FROM tasks.queue q
                USING d
                WHERE q.task_id = d.task_id
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
                WHERE q.task_id = d.task_id
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
        promote = sql("""
            UPDATE tasks.queue q
            SET state = 'ready', run_at = NULL
            WHERE q.task_id IN (
                SELECT o.task_id FROM tasks.queue o
                WHERE o.state IN ('scheduled', 'retrying') AND o.run_at <= clock_timestamp()
                ORDER BY o.run_at
                LIMIT :limit::integer
                FOR UPDATE OF o SKIP LOCKED
            )
            RETURNING q.topic_id
            """);
        // The attempt is kept: an expired run counts, since a poison task can take a node down.
        expire = sql("""
            WITH stale AS (
                SELECT o.task_id, o.topic_id, o.attempt, o.node_id, o.claimed_at
                FROM tasks.queue o
                WHERE o.state = 'active'
                  AND o.node_id <> :node::integer
                  AND o.node_id IN (SELECT id FROM tasks.node WHERE heartbeat_at < now() - :expiry::bigint * interval '1 millisecond')
                FOR UPDATE OF o SKIP LOCKED
            ),
            requeued AS (
                UPDATE tasks.queue q
                SET state = 'ready', node_id = NULL, claimed_at = NULL
                FROM stale
                WHERE q.task_id = stale.task_id
                RETURNING q.task_id, q.topic_id
            ),
            logged AS (
                INSERT INTO tasks.attempt (topic_id, task_id, attempt, node_id, started_at, result)
                SELECT s.topic_id, s.task_id, s.attempt, s.node_id, coalesce(s.claimed_at, now()), 'expired'
                FROM stale s JOIN requeued r ON r.task_id = s.task_id
            )
            SELECT topic_id FROM requeued
            """);
    }

    public String schema() {
        return schema;
    }

    private NamedSql sql(String text) {
        return NamedSql.parse(schema.equals(TaskSchema.DEFAULT_SCHEMA) ? text : text.replace("tasks.", schema + "."));
    }

    private static String pushSql(Insertion insertion) {
        // One statement: ids are drawn in input order, queue rows are inserted first so a dedup
        // conflict also skips the task row, then older rows are handled for the identifiers
        // that were actually queued. Every CTE sees the snapshot from before the statement, so
        // none of them can touch the rows this statement inserts.
        String cleanup = switch (insertion) {
            case APPEND -> "";
            case SUPERSEDE -> """
                ,
                superseded AS (
                    DELETE FROM tasks.queue q
                    WHERE q.task_id IN (
                        SELECT o.task_id FROM tasks.queue o
                        WHERE o.topic_id = :topic::integer
                          AND o.identifier IN (SELECT identifier FROM queued)
                          AND o.state IN ('ready', 'scheduled', 'retrying')
                        FOR UPDATE OF o SKIP LOCKED)
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
                    WHERE q.task_id IN (
                        SELECT o.task_id FROM tasks.queue o
                        WHERE o.topic_id = :topic::integer
                          AND o.identifier IN (SELECT identifier FROM queued)
                          AND o.state <> 'active'
                        FOR UPDATE OF o SKIP LOCKED)
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
                    WHERE q.task_id IN (
                        SELECT o.task_id FROM tasks.queue o
                        WHERE o.topic_id = :topic::integer
                          AND o.identifier IN (SELECT identifier FROM queued)
                          AND o.state <> 'active'
                        FOR UPDATE OF o SKIP LOCKED)
                    RETURNING q.task_id
                ),
                deleted_tasks AS (
                    DELETE FROM tasks.task t
                    WHERE t.topic_id = :topic::integer
                      AND (t.id IN (SELECT task_id FROM deleted)
                           OR t.id IN (
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
                            :priorities::integer[], :dedup::text[])
                    WITH ORDINALITY AS i(identifier, payload, payload_type, reference, parent_id, root_id,
                                         state, delay_ms, priority, dedup_key, ord)
            ),
            queued AS (
                INSERT INTO tasks.queue (task_id, topic_id, identifier, state, priority, run_at, dedup_key)
                SELECT id, :topic::integer, identifier, state::tasks.queue_state, priority,
                       CASE WHEN state = 'scheduled' THEN now() + delay_ms * interval '1 millisecond' END,
                       dedup_key
                FROM input
                WHERE state IS NOT NULL
                ORDER BY ord
                ON CONFLICT (topic_id, dedup_key) DO NOTHING
                RETURNING task_id, identifier
            ),
            stored AS (
                INSERT INTO tasks.task (topic_id, id, identifier, reference, payload, payload_type, parent_id, root_id, outcome, completed_at)
                SELECT :topic::integer, id, identifier, reference, payload, payload_type, parent_id, root_id,
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

    private static String pollSql(Ordering ordering) {
        String blocking = switch (ordering) {
            case FIFO -> null;
            case SINGULAR_BY_IDENTIFIER -> "'ready', 'retrying'";
            case SINGULAR_BY_IDENTIFIER_ON_FAILURE -> "'ready', 'retrying', 'failed'";
            case SINGULAR_BY_IDENTIFIER_UNTIL_SUCCESS -> "'ready', 'retrying', 'failed', 'suspended', 'scheduled'";
        };
        // Only tasks in the same topic count (finding #6), and only earlier ones (finding #7),
        // except that an active task blocks its identifier in both directions.
        String condition = blocking == null ? "" : """
                  AND NOT EXISTS (
                      SELECT 1 FROM tasks.queue b
                      WHERE b.topic_id = c.topic_id
                        AND b.identifier = c.identifier
                        AND b.task_id <> c.task_id
                        AND (b.state = 'active' OR (b.task_id < c.task_id AND b.state IN (%s))))
            """.formatted(blocking);
        return """
            WITH picked AS MATERIALIZED (
                SELECT c.task_id
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
                FROM picked
                WHERE q.task_id = picked.task_id
                RETURNING q.task_id, q.identifier, q.attempt, q.priority, q.claimed_at
            )
            SELECT c.task_id, c.identifier, c.attempt, c.priority, c.claimed_at,
                   t.payload, t.payload_type, t.reference, t.parent_id, t.root_id, t.created_at
            FROM claimed c
            JOIN tasks.task t ON t.id = c.task_id
            WHERE t.topic_id = :topic::integer
            ORDER BY c.priority DESC, c.task_id
            """;
    }

    @Override
    public int createTopic(Connection connection, String name) throws SQLException {
        try (PreparedStatement statement = createTopic.prepare(connection, NamedSql.parameters().set("name", name));
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    @Override
    public OptionalInt findTopic(Connection connection, String name) throws SQLException {
        try (PreparedStatement statement = findTopic.prepare(connection, NamedSql.parameters().set("name", name));
             ResultSet resultSet = statement.executeQuery()) {
            return resultSet.next() ? OptionalInt.of(resultSet.getInt(1)) : OptionalInt.empty();
        }
    }

    @Override
    public List<Pushed> push(Connection connection, int topicId, Insertion insertion, List<NewTask> tasks) throws SQLException {
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
            .array("dedup", "text", dedup);
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

    @Override
    public Claim claim(Connection connection, ClaimRequest request) throws SQLException {
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
                    instant(resultSet, "claimed_at")
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
            ensureAdmission(connection, admission.name(), admission.burst());
        }
        throw new IllegalStateException("No admission state for rate limit " + admission.name());
    }

    @Override
    public void complete(Connection connection, int topicId, int nodeId, List<Completion> completions) throws SQLException {
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
    public int release(Connection connection, int topicId, int nodeId, List<Completion> completions) throws SQLException {
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
    public List<Integer> promoteDue(Connection connection, int limit) throws SQLException {
        return topics(promote, connection, NamedSql.parameters().set("limit", limit));
    }

    @Override
    public List<Integer> requeueExpired(Connection connection, int nodeId, Duration expiry) throws SQLException {
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
    public int registerNode(Connection connection, String name, String version) throws SQLException {
        try (PreparedStatement statement = registerNode.prepare(connection, NamedSql.parameters().set("name", name).set("version", version));
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    @Override
    public void heartbeat(Connection connection, int nodeId) throws SQLException {
        try (PreparedStatement statement = heartbeat.prepare(connection, NamedSql.parameters().set("node", nodeId))) {
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Node " + nodeId + " is not registered");
            }
        }
    }

    @Override
    public void ensureAdmission(Connection connection, String name, int burst) throws SQLException {
        try (PreparedStatement statement = ensureAdmission.prepare(connection, NamedSql.parameters().set("name", name).set("burst", (double) burst))) {
            statement.executeUpdate();
        }
    }

    private static Instant instant(ResultSet resultSet, String column) throws SQLException {
        OffsetDateTime value = resultSet.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    @Override
    public String toString() {
        return "PostgresTaskStore[" + Objects.requireNonNull(schema) + "]";
    }
}
