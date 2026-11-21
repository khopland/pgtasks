package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import io.github.khopland.pgtasks.TopicRef;

/**
 * Copies the tasks of a Task 1.x {@code TASK} table, in the same database, into the tasks
 * schema: the move from 1.x without losing what is queued. Stop every 1.x node first, apply the
 * tasks schema (start a 2.0 engine once, or {@link TaskSchema#migrate}), then run it once.
 *
 * <pre>{@code
 * LegacyImport.Report report = LegacyImport.run(dataSource, LegacyImport.Options.defaults());
 * }</pre>
 *
 * <ul>
 *   <li>Each 1.x topic becomes a topic, its name lower-cased by default
 *       ({@link Options#withTopicNames}); names that aren't valid topic names, or that two topics
 *       would share, stop the import before anything is copied.</li>
 *   <li>READY, ACTIVE and EXPIRED tasks become ready, or delayed until their {@code SCHEDULED}
 *       time if it is still to come; SUSPENDED stay suspended, waiting for a junction or an
 *       operator; FAILED become dead letters, to revive or discard. In sequence order, so ordering
 *       by identifier carries on as it was.</li>
 *   <li>Finished tasks (SUCCEEDED, FILTERED, REDUNDANT, RECREATED) are copied only with
 *       {@link Options#withHistory()}, then kept as the topic's retention says.</li>
 *   <li>{@code INPUT} becomes the payload, as is, with no payload type: a topic of strings
 *       reads it directly, any other needs a codec that reads the 1.x format.
 *       {@code OUTPUT}, {@code REFERENCE} and {@code CREATED} come along.</li>
 * </ul>
 *
 * Each topic is copied in one transaction. A topic that already has tasks in the tasks schema is
 * skipped and reported, so running it again does no harm. The 1.x table is only read.
 */
public final class LegacyImport {

    private static final Pattern TABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?");

    /** 1.x states by ordinal: ACTIVE 0, READY 1, EXPIRED 2, FAILED 3, SUSPENDED 4, FILTERED 5, SUCCEEDED 6, RECREATED 7, REDUNDANT 8. */
    private static final String COPY = """
        WITH legacy AS MATERIALIZED (
            SELECT nextval('tasks.task_id_seq') AS id, l.*
            FROM (SELECT sequence, identifier, state::integer AS state, created, completed, input, output, reference, scheduled
                  FROM %s
                  WHERE topic = :legacy AND (state::integer <= 4 OR :history::boolean)
                  ORDER BY sequence) l
        ),
        stored AS (
            INSERT INTO tasks.task (id, topic_id, identifier, reference, payload, created_at, outcome, completed_at, output, attempts, error)
            SELECT id, :topic::integer, identifier, reference, input, created AT TIME ZONE 'UTC',
                   CASE state WHEN 5 THEN 'filtered' WHEN 6 THEN 'succeeded' WHEN 7 THEN 'recreated' WHEN 8 THEN 'redundant' END::tasks.task_outcome,
                   CASE WHEN state > 4 THEN coalesce(completed, created) AT TIME ZONE 'UTC' END,
                   output, 0,
                   CASE WHEN state = 3 THEN jsonb_build_object('message', 'Failed in Task 1.x, before the import', 'type', 'task1.failed') END
            FROM legacy
        ),
        queued AS (
            INSERT INTO tasks.queue (task_id, topic_id, identifier, state, run_at, failures, first_failed_at)
            SELECT id, :topic::integer, identifier,
                   (CASE WHEN state = 3 THEN 'failed'
                         WHEN state = 4 THEN 'suspended'
                         WHEN scheduled AT TIME ZONE 'UTC' > now() THEN 'scheduled'
                         ELSE 'ready' END)::tasks.queue_state,
                   CASE WHEN state <= 2 AND scheduled AT TIME ZONE 'UTC' > now() THEN scheduled AT TIME ZONE 'UTC' END,
                   CASE WHEN state = 3 THEN 1 ELSE 0 END,
                   CASE WHEN state = 3 THEN coalesce(completed, created) AT TIME ZONE 'UTC' END
            FROM legacy
            WHERE state <= 4
        )
        SELECT count(*) FILTER (WHERE state <= 4), count(*) FILTER (WHERE state > 4) FROM legacy
        """;

    private LegacyImport() {
    }

    /**
     * @param table       the 1.x table, {@code task} by default, schema-qualified if it isn't on the search path
     * @param history     whether finished tasks are copied too
     * @param topicNames  the topic name for each 1.x topic
     */
    public record Options(String table, boolean history, Function<String, String> topicNames) {

        public Options {
            Objects.requireNonNull(table, "table");
            Objects.requireNonNull(topicNames, "topicNames");
            if (!TABLE.matcher(table).matches()) {
                throw new IllegalArgumentException("Not a table name: '" + table + "'");
            }
        }

        /** The {@code task} table, unfinished tasks only, topic names lower-cased. */
        public static Options defaults() {
            return new Options("task", false, LegacyImport::topicName);
        }

        public Options withTable(String table) {
            return new Options(table, history, topicNames);
        }

        /** Copies finished tasks too, as history. */
        public Options withHistory() {
            return new Options(table, true, topicNames);
        }

        public Options withTopicNames(Function<String, String> topicNames) {
            return new Options(table, history, topicNames);
        }
    }

    /**
     * What was copied, by topic.
     *
     * @param unfinished ready, delayed, suspended and dead-lettered tasks copied, by new topic name
     * @param finished   finished tasks copied as history, by new topic name
     * @param skipped    topics left out because they already had tasks
     */
    public record Report(Map<String, Long> unfinished, Map<String, Long> finished, List<String> skipped) {

        public long total() {
            return unfinished.values().stream().mapToLong(Long::longValue).sum() + finished.values().stream().mapToLong(Long::longValue).sum();
        }
    }

    /** The default topic name: lower case, with characters a topic name can't have replaced by '-'. */
    public static String topicName(String legacy) {
        String name = legacy.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9._-]", "-");
        return name.isEmpty() || !Character.isLetterOrDigit(name.charAt(0)) ? "t" + name : name;
    }

    public static Report run(DataSource dataSource, Options options) throws SQLException {
        Map<String, String> topics = topics(dataSource, options);
        Map<String, Long> unfinished = new LinkedHashMap<>(), finished = new LinkedHashMap<>();
        List<String> skipped = new ArrayList<>();
        NamedSql copy = NamedSql.parse(COPY.formatted(options.table()));
        for (Map.Entry<String, String> topic : topics.entrySet()) {
            try (Connection connection = dataSource.getConnection()) {
                connection.setAutoCommit(false);
                try {
                    int id = topicId(connection, topic.getValue());
                    if (hasTasks(connection, id)) {
                        skipped.add(topic.getValue());
                        connection.rollback();
                        continue;
                    }
                    try (PreparedStatement statement = copy.prepare(connection, NamedSql.parameters()
                        .set("legacy", topic.getKey()).set("topic", id).set("history", options.history()));
                         ResultSet counts = statement.executeQuery()) {
                        counts.next();
                        unfinished.put(topic.getValue(), counts.getLong(1));
                        finished.put(topic.getValue(), counts.getLong(2));
                    }
                    connection.commit();
                } catch (SQLException | RuntimeException e) {
                    connection.rollback();
                    throw e;
                }
            }
        }
        return new Report(Map.copyOf(unfinished), Map.copyOf(finished), List.copyOf(skipped));
    }

    /** 1.x topic to topic name, checked before anything is copied. */
    private static Map<String, String> topics(DataSource dataSource, Options options) throws SQLException {
        Map<String, String> topics = new LinkedHashMap<>();
        Map<String, String> byName = new LinkedHashMap<>();
        List<String> problems = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT DISTINCT topic FROM " + options.table() + " ORDER BY topic");
             ResultSet result = statement.executeQuery()) {
            while (result.next()) {
                String legacy = result.getString(1);
                String name = options.topicNames().apply(legacy);
                try {
                    TopicRef.of(name, Object.class);
                } catch (IllegalArgumentException e) {
                    problems.add(legacy + ": " + e.getMessage());
                    continue;
                }
                String other = byName.putIfAbsent(name, legacy);
                if (other != null) {
                    problems.add(legacy + " and " + other + " would both become topic " + name);
                }
                topics.put(legacy, name);
            }
        }
        if (!problems.isEmpty()) {
            throw new IllegalArgumentException("Can't import the 1.x topics as they are named; give Options.withTopicNames(...) a mapping:\n - "
                + String.join("\n - ", problems));
        }
        return topics;
    }

    private static int topicId(Connection connection, String name) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT tasks.create_topic(?)")) {
            statement.setString(1, name);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getInt(1);
            }
        }
    }

    private static boolean hasTasks(Connection connection, int topicId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT EXISTS (SELECT 1 FROM tasks.task WHERE topic_id = ?)")) {
            statement.setInt(1, topicId);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getBoolean(1);
            }
        }
    }
}
