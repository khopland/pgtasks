package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The built-in runner. Takes a session advisory lock, keeps its history in
 * {@code tasks.schema_history}, runs each versioned file once in its own transaction, re-runs
 * repeatable files whose checksum changed, and refuses to run if an applied versioned file has
 * changed. If a newer node has already moved the schema ahead, it changes nothing and only
 * checks that this version may run against it.
 */
final class SchemaMigrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaMigrator.class);

    private static final String LOCK = "SELECT pg_advisory_lock(hashtextextended('tasks.schema_history', 0))";
    private static final String UNLOCK = "SELECT pg_advisory_unlock(hashtextextended('tasks.schema_history', 0))";

    private static final String HISTORY = """
        CREATE TABLE IF NOT EXISTS tasks.schema_history (
            installed_rank  integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            version         integer,
            script          text NOT NULL,
            checksum        text NOT NULL,
            applied_at      timestamptz NOT NULL DEFAULT now(),
            applied_by      text NOT NULL DEFAULT current_user,
            execution_ms    integer NOT NULL
        )""";

    private final List<SchemaScript> versioned;
    private final List<SchemaScript> repeatable;

    SchemaMigrator(List<SchemaScript> scripts) {
        this.versioned = scripts.stream().filter(script -> !script.repeatable()).sorted(Comparator.comparingInt(SchemaScript::number)).toList();
        this.repeatable = scripts.stream().filter(SchemaScript::repeatable).sorted(Comparator.comparing(SchemaScript::description)).toList();
        for (int index = 1; index < versioned.size(); index++) {
            if (versioned.get(index).number() == versioned.get(index - 1).number()) {
                throw new IllegalArgumentException("Two schema files have version " + versioned.get(index).number());
            }
        }
        if (versioned.isEmpty()) {
            throw new IllegalArgumentException("No versioned schema files");
        }
    }

    int latest() {
        return versioned.getLast().number();
    }

    TaskSchema.Status migrate(Connection connection) throws SQLException {
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(true);
        execute(connection, LOCK);
        try {
            execute(connection, "CREATE SCHEMA IF NOT EXISTS tasks");
            execute(connection, HISTORY);
            Map<Integer, String> appliedVersions = new HashMap<>();
            Map<String, String> repeatableChecksums = new HashMap<>();
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("SELECT version, script, checksum FROM tasks.schema_history ORDER BY installed_rank")) {
                while (resultSet.next()) {
                    Integer version = resultSet.getObject(1, Integer.class);
                    if (version == null) {
                        repeatableChecksums.put(resultSet.getString(2), resultSet.getString(3));
                    } else {
                        appliedVersions.put(version, resultSet.getString(3));
                    }
                }
            }
            for (Map.Entry<Integer, String> applied : appliedVersions.entrySet()) {
                Optional<SchemaScript> local = versioned.stream().filter(script -> script.number() == applied.getKey()).findFirst();
                if (local.isEmpty() && applied.getKey() <= latest()) {
                    throw new TaskSchemaException("Version " + applied.getKey() + " of the tasks schema was applied, but this library has no such file. "
                        + "Applied versioned files must never be removed or renumbered.");
                }
                if (local.isPresent() && !local.get().checksum().equals(applied.getValue())) {
                    throw new TaskSchemaException(local.get().name() + " has changed since it was applied (checksum "
                        + applied.getValue() + " in tasks.schema_history, " + local.get().checksum() + " now). "
                        + "Applied versioned files must never change; add a new versioned file instead.");
                }
            }
            Optional<TaskSchema.Meta> meta = TaskSchema.readMeta(connection);
            if (meta.isPresent() && appliedVersions.isEmpty()) {
                throw new TaskSchemaException("The tasks schema is at version " + meta.get().version() + ", but tasks.schema_history doesn't know "
                    + "how it got there: another tool applied it. Keep applying it with that tool (Flyway, Liquibase or your DBA pipeline) "
                    + "and run the library with schema mode VALIDATE.");
            }
            if (meta.isPresent() && meta.get().version() > latest()) {
                // A newer node has migrated already. Never downgrade its functions or views.
                TaskSchema.Status status = TaskSchema.check(meta, latest(), List.of());
                LOGGER.info("The tasks schema is at version {}, ahead of this library's {} and compatible with it; leaving it as it is",
                    status.version(), latest());
                return status;
            }
            List<String> applied = new ArrayList<>();
            for (SchemaScript script : versioned) {
                if (!appliedVersions.containsKey(script.version())) {
                    apply(connection, script);
                    applied.add(script.name());
                }
            }
            for (SchemaScript script : repeatable) {
                if (!script.checksum().equals(repeatableChecksums.get(script.name()))) {
                    apply(connection, script);
                    applied.add(script.name());
                }
            }
            if (!applied.isEmpty()) {
                LOGGER.info("Applied to the tasks schema: {}", String.join(", ", applied));
            }
            return TaskSchema.check(TaskSchema.readMeta(connection), latest(), applied);
        } finally {
            try {
                execute(connection, UNLOCK);
            } finally {
                connection.setAutoCommit(autoCommit);
            }
        }
    }

    private void apply(Connection connection, SchemaScript script) throws SQLException {
        long started = System.nanoTime();
        List<String> statements = script.statements();
        if (!script.transactional()) {
            // Each statement on its own; the script must be safe to run again if one fails.
            for (int index = 0; index < statements.size(); index++) {
                run(connection, script, statements, index);
            }
            record(connection, script, started);
            return;
        }
        connection.setAutoCommit(false);
        try {
            for (int index = 0; index < statements.size(); index++) {
                run(connection, script, statements, index);
            }
            if (!script.repeatable()) {
                Optional<TaskSchema.Meta> meta = TaskSchema.readMeta(connection);
                if (meta.isEmpty() || meta.get().version() != script.number()) {
                    throw new TaskSchemaException(script.name() + " must end by recording version " + script.version()
                        + " in tasks.meta, but tasks.meta says " + meta.map(m -> String.valueOf(m.version())).orElse("nothing"));
                }
            }
            record(connection, script, started);
            connection.commit();
        } catch (SQLException | RuntimeException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    private static void run(Connection connection, SchemaScript script, List<String> statements, int index) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(statements.get(index));
        } catch (SQLException e) {
            throw new TaskSchemaException("Could not apply " + script.name() + ": statement " + (index + 1) + " of " + statements.size()
                + " failed: " + e.getMessage(), e);
        }
    }

    private static void record(Connection connection, SchemaScript script, long started) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
            "INSERT INTO tasks.schema_history (version, script, checksum, execution_ms) VALUES (?, ?, ?, ?)")) {
            statement.setObject(1, script.version());
            statement.setString(2, script.name());
            statement.setString(3, script.checksum());
            statement.setInt(4, (int) ((System.nanoTime() - started) / 1_000_000));
            statement.executeUpdate();
        }
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
