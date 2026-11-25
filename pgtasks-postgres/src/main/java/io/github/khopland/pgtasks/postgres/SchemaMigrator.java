package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The built-in runner. Takes a session advisory lock per history table, keeps its history in the
 * schema's own {@code schema_history} (or an extension's own table), runs each versioned file once
 * in its own transaction, re-runs repeatable files whose checksum changed unless a higher revision
 * of them is applied, and refuses to run if an applied versioned file has changed. If a newer node
 * has already moved the schema ahead, it changes nothing and only checks that this version may run
 * against it.
 */
final class SchemaMigrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaMigrator.class);

    /** How long one statement waits for a lock before the runner gives up on that attempt. */
    static final Duration LOCK_TIMEOUT = Duration.ofSeconds(5);

    /** Attempts per file before startup fails. */
    static final int ATTEMPTS = 5;

    /** The wait after the first lock timeout; it doubles with each one after. */
    static final Duration BACKOFF = Duration.ofSeconds(1);

    /** Postgres' SQLState for a lock wait cancelled by {@code lock_timeout}. */
    private static final String LOCK_NOT_AVAILABLE = "55P03";

    private final List<SchemaScript> versioned;
    private final List<SchemaScript> repeatable;
    private final String schema;
    private final String historyTable;
    private final boolean usesMeta;
    private final Duration lockTimeout;
    private final int attempts;
    private final Duration backoff;

    SchemaMigrator(List<SchemaScript> scripts, String schema) {
        this(scripts, schema, "schema_history", true, LOCK_TIMEOUT, ATTEMPTS, BACKOFF);
    }

    /** A runner for an extension's own files: its own history table, and no core meta table to check. */
    SchemaMigrator(List<SchemaScript> scripts, String schema, String historyTable) {
        this(scripts, schema, historyTable, false, LOCK_TIMEOUT, ATTEMPTS, BACKOFF);
    }

    SchemaMigrator(List<SchemaScript> scripts, String schema, Duration lockTimeout, int attempts, Duration backoff) {
        this(scripts, schema, "schema_history", true, lockTimeout, attempts, backoff);
    }

    /**
     * @param lockTimeout how long each statement may wait for a lock. DDL on a busy table, such as
     *                    a trigger on the queue, waits behind every open transaction that touched
     *                    it, and every push and claim then waits behind the DDL; the timeout bounds
     *                    that stall
     * @param attempts    tries per file before giving up, each after a lock timeout
     * @param backoff     the wait after the first lock timeout, doubled after each later one
     */
    SchemaMigrator(List<SchemaScript> scripts, String schema, String historyTable, boolean usesMeta,
                   Duration lockTimeout, int attempts, Duration backoff) {
        if (lockTimeout.toMillis() < 1 || attempts < 1 || backoff.isNegative()) {
            throw new IllegalArgumentException("Need a lock timeout of at least 1 ms, at least one attempt and a backoff of zero or more");
        }
        this.lockTimeout = lockTimeout;
        this.attempts = attempts;
        this.backoff = backoff;
        this.schema = TaskSchema.validName(schema);
        this.historyTable = TaskSchema.validName(historyTable);
        this.usesMeta = usesMeta;
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
        execute(connection, "SELECT pg_advisory_lock(hashtextextended('" + schema + "." + historyTable + "', 0))");
        String previousLockTimeout = null;
        try {
            // Set after taking the advisory lock: waiting for another runner to finish is fine.
            previousLockTimeout = setLockTimeout(connection, lockTimeout.toMillis() + "ms");
            execute(connection, "CREATE SCHEMA IF NOT EXISTS " + schema);
            execute(connection, history(schema, historyTable));
            execute(connection, "ALTER TABLE " + schema + "." + historyTable + " ADD COLUMN IF NOT EXISTS revision integer");
            Map<Integer, String> appliedVersions = new HashMap<>();
            Map<String, Recorded> repeatableApplied = new HashMap<>();
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("SELECT version, script, checksum, coalesce(revision, 0) FROM " + schema
                     + "." + historyTable + " ORDER BY installed_rank")) {
                while (resultSet.next()) {
                    Integer version = resultSet.getObject(1, Integer.class);
                    if (version == null) {
                        repeatableApplied.put(resultSet.getString(2), new Recorded(resultSet.getString(3), resultSet.getInt(4)));
                    } else {
                        appliedVersions.put(version, resultSet.getString(3));
                    }
                }
            }
            for (Map.Entry<Integer, String> applied : appliedVersions.entrySet()) {
                Optional<SchemaScript> local = versioned.stream().filter(script -> script.number() == applied.getKey()).findFirst();
                if (local.isEmpty() && applied.getKey() <= latest()) {
                    throw new TaskSchemaException("Version " + applied.getKey() + " of the " + schema + "." + historyTable + " history was applied, but these files have no such version. "
                        + "Applied versioned files must never be removed or renumbered.");
                }
                if (local.isPresent() && !local.get().checksum().equals(applied.getValue())) {
                    throw new TaskSchemaException(local.get().name() + " has changed since it was applied (checksum "
                        + applied.getValue() + " in " + schema + "." + historyTable + ", " + local.get().checksum() + " now). "
                        + "Applied versioned files must never change; add a new versioned file instead.");
                }
            }
            Optional<TaskSchema.Meta> meta = usesMeta ? TaskSchema.readMeta(connection, schema) : Optional.empty();
            if (usesMeta && meta.isPresent() && appliedVersions.isEmpty()) {
                throw new TaskSchemaException("The " + schema + " schema is at version " + meta.get().version() + ", but " + schema
                    + "." + historyTable + " doesn't know "
                    + "how it got there: another tool applied it. Keep applying it with that tool (Flyway, Liquibase or your DBA pipeline) "
                    + "and run the library with schema mode VALIDATE.");
            }
            if (!usesMeta && appliedVersions.keySet().stream().mapToInt(Integer::intValue).max().orElse(0) > latest()) {
                // A newer node applied this extension's files. Leave them alone.
                int version = appliedVersions.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
                LOGGER.info("{} is at version {}, ahead of this library's {}; leaving it as it is", historyTable, version, latest());
                return new TaskSchema.Status(version, version, latest(), List.of());
            }
            if (usesMeta && meta.isPresent() && meta.get().version() > latest()) {
                // A newer node has migrated already. Never downgrade its functions or views.
                TaskSchema.Status status = TaskSchema.check(schema, meta, latest(), List.of());
                LOGGER.info("The {} schema is at version {}, ahead of this library's {} and compatible with it; leaving it as it is",
                    schema, status.version(), latest());
                return status;
            }
            List<String> applied = new ArrayList<>();
            for (SchemaScript script : versioned) {
                if (!appliedVersions.containsKey(script.version())) {
                    applyRetrying(connection, script);
                    applied.add(script.name());
                }
            }
            for (SchemaScript script : repeatable) {
                if (shouldApply(script, repeatableApplied.get(script.name()))) {
                    applyRetrying(connection, script);
                    applied.add(script.name());
                }
            }
            if (!applied.isEmpty()) {
                LOGGER.info("Applied to the {} schema: {}", schema, String.join(", ", applied));
            }
            return usesMeta
                ? TaskSchema.check(schema, TaskSchema.readMeta(connection, schema), latest(), applied)
                : new TaskSchema.Status(latest(), latest(), latest(), applied);
        } finally {
            try {
                if (previousLockTimeout != null) {
                    setLockTimeout(connection, previousLockTimeout);
                }
            } finally {
                try {
                    execute(connection, "SELECT pg_advisory_unlock(hashtextextended('" + schema + "." + historyTable + "', 0))");
                } finally {
                    connection.setAutoCommit(autoCommit);
                }
            }
        }
    }

    /** Sets the session's lock_timeout and returns the one it had. */
    private static String setLockTimeout(Connection connection, String value) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
            "SELECT current_setting('lock_timeout'), set_config('lock_timeout', ?, false)")) {
            statement.setString(1, value);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getString(1);
            }
        }
    }

    /**
     * Applies a file, trying again when a statement timed out waiting for a lock. A transactional
     * file was rolled back; a non-transactional one runs again from its first statement, which its
     * convention allows. The waits stay short, so that pushes and claims queued behind the DDL
     * aren't held up for long, and startup fails once the attempts are used up.
     */
    private void applyRetrying(Connection connection, SchemaScript script) throws SQLException {
        for (int attempt = 1; ; attempt++) {
            try {
                apply(connection, script);
                return;
            } catch (SQLException | TaskSchemaException e) {
                if (!lockTimedOut(e)) {
                    throw e;
                }
                String blockers = blockers(connection);
                if (attempt >= attempts) {
                    throw new TaskSchemaException("Could not apply " + script.name() + " to the " + schema + " schema: it waited more than "
                        + lockTimeout.toMillis() + " ms for a lock on each of " + attempts + " attempts, and gave up rather than hold up "
                        + "every push and claim queued behind it. " + blockers + " Long transactions that touch the "
                        + schema + " tables, such as an application transaction that pushed and stays open, block schema changes; "
                        + "end them, or start again when they have finished.", e);
                }
                Duration wait = backoff.multipliedBy(1L << Math.min(attempt - 1, 16));
                LOGGER.warn("Applying {} to the {} schema timed out waiting for a lock (attempt {} of {}); trying again in {} ms. {}",
                    script.name(), schema, attempt, attempts, wait.toMillis(), blockers);
                try {
                    Thread.sleep(wait);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new TaskSchemaException("Interrupted while waiting to apply " + script.name() + " again", interrupted);
                }
            }
        }
    }

    private static boolean lockTimedOut(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sql && LOCK_NOT_AVAILABLE.equals(sql.getSQLState())) {
                return true;
            }
        }
        return false;
    }

    /** The open transactions that hold locks on the schema's tables, oldest first: the likely blockers, for the message. */
    private String blockers(Connection connection) {
        List<String> found = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement("""
            SELECT a.pid, a.usename, a.application_name, a.state,
                   floor(extract(epoch FROM now() - a.xact_start))::bigint, left(regexp_replace(a.query, '\\s+', ' ', 'g'), 120)
            FROM pg_stat_activity a
            WHERE a.pid <> pg_backend_pid()
              AND a.xact_start IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM pg_locks l
                  JOIN pg_class c ON c.oid = l.relation
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE l.pid = a.pid AND l.granted AND n.nspname = ?)
            ORDER BY a.xact_start
            LIMIT 5
            """)) {
            statement.setString(1, schema);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    found.add("pid " + resultSet.getLong(1) + " (user " + resultSet.getString(2)
                        + (resultSet.getString(3) == null || resultSet.getString(3).isEmpty() ? "" : ", application '" + resultSet.getString(3) + "'")
                        + ", " + resultSet.getString(4) + ", transaction open for " + resultSet.getLong(5) + " s, last statement: "
                        + resultSet.getString(6) + ")");
                }
            }
        } catch (SQLException e) {
            return "Could not list the transactions that hold locks on the " + schema + " tables: " + e.getMessage() + ".";
        }
        if (found.isEmpty()) {
            return "No open transaction holds a lock on the " + schema + " tables now.";
        }
        return "Open transactions holding locks on the " + schema + " tables, oldest first: " + String.join("; ", found) + ".";
    }

    /** The latest row {@code schema_history} holds for a repeatable file; revision 0 if an older runner recorded it. */
    private record Recorded(String checksum, int revision) {
    }

    /**
     * Whether to apply a repeatable file: when it was never applied, when it is a higher revision
     * than the one applied last, or when it is the same revision with other content, which only a
     * file changed without raising its revision does (SchemaFreezeTest stops shipped files doing
     * so). Never over a higher revision: a newer node applied that, and this one would roll it back.
     */
    private boolean shouldApply(SchemaScript script, @Nullable Recorded recorded) {
        if (recorded == null) {
            return true;
        }
        if (script.revision() < recorded.revision()) {
            if (!script.checksum().equals(recorded.checksum())) {
                LOGGER.info("{} is at revision {} in the {} schema, newer than this library's revision {}; leaving it as it is",
                    script.name(), recorded.revision(), schema, script.revision());
            }
            return false;
        }
        if (script.revision() == recorded.revision() && !script.checksum().equals(recorded.checksum())) {
            LOGGER.warn("{} differs from what the {} schema has applied at the same revision {}; applying it again. "
                + "Raise the revision in the file's header with every change to it.", script.name(), schema, script.revision());
            return true;
        }
        return script.revision() > recorded.revision();
    }

    private void apply(Connection connection, SchemaScript script) throws SQLException {
        long started = System.nanoTime();
        List<String> statements = script.statements(schema);
        if (!script.transactional()) {
            // Each statement on its own; the script must be safe to run again if one fails.
            for (int index = 0; index < statements.size(); index++) {
                run(connection, script, statements, index);
            }
            record(connection, schema, historyTable, script, started);
            return;
        }
        connection.setAutoCommit(false);
        try {
            for (int index = 0; index < statements.size(); index++) {
                run(connection, script, statements, index);
            }
            if (usesMeta && !script.repeatable()) {
                Optional<TaskSchema.Meta> meta = TaskSchema.readMeta(connection, schema);
                if (meta.isEmpty() || meta.get().version() != script.number()) {
                    throw new TaskSchemaException(script.name() + " must end by recording version " + script.version()
                        + " in " + schema + ".meta, but it says " + meta.map(m -> String.valueOf(m.version())).orElse("nothing"));
                }
            }
            record(connection, schema, historyTable, script, started);
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

    private static void record(Connection connection, String schema, String historyTable, SchemaScript script, long started) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
            "INSERT INTO " + schema + "." + historyTable + " (version, script, checksum, execution_ms, revision) VALUES (?, ?, ?, ?, ?)")) {
            statement.setObject(1, script.version());
            statement.setString(2, script.name());
            statement.setString(3, script.checksum());
            statement.setInt(4, (int) ((System.nanoTime() - started) / 1_000_000));
            statement.setObject(5, script.repeatable() ? script.revision() : null, java.sql.Types.INTEGER);
            statement.executeUpdate();
        }
    }

    private static String history(String schema, String table) {
        return """
            CREATE TABLE IF NOT EXISTS %s.%s (
                installed_rank  integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                version         integer,
                script          text NOT NULL,
                checksum        text NOT NULL,
                applied_at      timestamptz NOT NULL DEFAULT now(),
                applied_by      text NOT NULL DEFAULT current_user,
                execution_ms    integer NOT NULL
            )""".formatted(schema, table);
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
