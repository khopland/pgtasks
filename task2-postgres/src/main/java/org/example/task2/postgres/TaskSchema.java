package org.example.task2.postgres;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

/**
 * The {@code tasks} schema. It ships as plain SQL files under {@value #LOCATION}, so the library
 * depends on no migration tool and every consumer picks how to apply them:
 *
 * <ul>
 *   <li>nothing in particular: the built-in runner applies them at startup ({@link SchemaMode#MIGRATE},
 *       the default, or {@link #migrate} directly);</li>
 *   <li>Flyway: a separate Flyway instance, so the version numbers don't clash with the
 *       application's own migrations, and {@link SchemaMode#VALIDATE}:
 *       <pre>{@code
 * Flyway.configure()
 *     .dataSource(dataSource)
 *     .locations("classpath:org/example/task2/postgres/db")
 *     .schemas("tasks")
 *     .table("flyway_task_history")
 *     .load()
 *     .migrate();
 *       }</pre></li>
 *   <li>Liquibase: include {@value #LIQUIBASE_CHANGELOG} from the application's changelog. It is a
 *       plain XML resource that wraps the same files with {@code sqlFile} ({@code runOnChange} for
 *       the repeatable ones), so it adds no dependency. Then {@link SchemaMode#VALIDATE};</li>
 *   <li>a DBA pipeline: the files in order, as the schema owner, and {@link SchemaMode#VALIDATE}.</li>
 * </ul>
 *
 * <p>Conventions every file follows, so all four paths behave the same:
 * {@code V<version>__<description>.sql} for tables, types and indexes, applied once, each ending
 * by recording its version and the lowest version it stays compatible with in {@code tasks.meta};
 * {@code R__<description>.sql} for functions, views and triggers, idempotent and re-applied when
 * they change; and a {@value SchemaScript#NO_TRANSACTION} header comment for scripts that must
 * run outside a transaction, such as {@code CREATE INDEX CONCURRENTLY}.
 *
 * <p>Rights: with {@code MIGRATE} the application's user owns the schema. Otherwise the runtime
 * user needs USAGE on the schema, SELECT, INSERT, UPDATE and DELETE on its tables, USAGE on
 * {@code task_id_seq}, and EXECUTE on {@code create_topic}. No DDL rights: a topic is a row.
 */
public final class TaskSchema {

    public static final String SCHEMA = "tasks";

    public static final String LOCATION = "org/example/task2/postgres/db";

    public static final String LIQUIBASE_CHANGELOG = LOCATION + "/changelog.xml";

    /** The files, in the order a DBA pipeline runs them: versioned by version, then repeatable by name. */
    static final List<String> FILES = List.of(
        "V001__tasks_schema.sql",
        "R__create_topic.sql",
        "R__notify_limit.sql",
        "R__notify_ready.sql",
        "R__task_view.sql"
    );

    /** The schema version this library is built for. */
    public static final int VERSION = 1;

    /** What {@code tasks.meta} records. */
    public record Meta(int version, int compatibleFrom) {
    }

    /**
     * @param version        the schema's version
     * @param compatibleFrom the oldest library schema version that may run against it
     * @param expected       the version this library is built for
     * @param applied        the files the runner applied just now
     */
    public record Status(int version, int compatibleFrom, int expected, List<String> applied) {
    }

    private TaskSchema() {
    }

    /** Applies whatever is missing with the built-in runner, then checks the version. */
    public static Status migrate(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            return new SchemaMigrator(scripts()).migrate(connection);
        }
    }

    /** Only checks {@code tasks.meta}; for schemas that another tool applies. */
    public static Status validate(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            return check(readMeta(connection), VERSION, List.of());
        }
    }

    public static Optional<Status> prepare(DataSource dataSource, SchemaMode mode) throws SQLException {
        return switch (mode) {
            case MIGRATE -> Optional.of(migrate(dataSource));
            case VALIDATE -> Optional.of(validate(dataSource));
            case NONE -> Optional.empty();
        };
    }

    /** The schema files from the classpath, in order. */
    static List<SchemaScript> scripts() {
        List<SchemaScript> scripts = new ArrayList<>();
        for (String name : FILES) {
            String path = LOCATION + "/" + name;
            try (InputStream input = TaskSchema.class.getClassLoader().getResourceAsStream(path)) {
                if (input == null) {
                    throw new IllegalStateException("Missing schema file on the classpath: " + path);
                }
                scripts.add(SchemaScript.of(name, new String(input.readAllBytes(), StandardCharsets.UTF_8)));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return scripts;
    }

    static Optional<Meta> readMeta(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet exists = statement.executeQuery("SELECT to_regclass('tasks.meta') IS NOT NULL")) {
                exists.next();
                if (!exists.getBoolean(1)) {
                    return Optional.empty();
                }
            }
            try (ResultSet resultSet = statement.executeQuery("SELECT version, compatible_from FROM tasks.meta")) {
                return resultSet.next() ? Optional.of(new Meta(resultSet.getInt(1), resultSet.getInt(2))) : Optional.empty();
            }
        }
    }

    /**
     * The one version check every path shares. The schema must be at the expected version, or
     * one version ahead if it marks itself compatible, so old and new nodes can run side by side
     * during a rolling deploy.
     */
    static Status check(Optional<Meta> found, int expected, List<String> applied) {
        if (found.isEmpty()) {
            throw new TaskSchemaException("The tasks schema doesn't exist in this database yet (there is no tasks.meta). Either let the "
                + "library create it at startup with schema mode MIGRATE, or apply the SQL files in classpath:" + LOCATION
                + " with Flyway, Liquibase or your DBA pipeline before starting, and keep schema mode VALIDATE.");
        }
        Meta meta = found.get();
        if (meta.version() < expected) {
            throw new TaskSchemaException("The tasks schema is at version " + meta.version() + ", but this library needs version " + expected
                + ". Apply the newer files from classpath:" + LOCATION + ": with schema mode MIGRATE the library does it at startup; "
                + "with Flyway, Liquibase or a DBA pipeline, run them before deploying this version.");
        }
        if (meta.version() > expected + 1) {
            throw new TaskSchemaException("The tasks schema is at version " + meta.version() + ", more than one version ahead of this library ("
                + expected + "). A rolling deploy may only run one schema version ahead; upgrade this application.");
        }
        if (meta.compatibleFrom() > expected) {
            throw new TaskSchemaException("The tasks schema is at version " + meta.version() + " and only compatible with library schema version "
                + meta.compatibleFrom() + " and later, but this library uses version " + expected
                + ". A newer release migrated it; upgrade this application or restore the schema.");
        }
        return new Status(meta.version(), meta.compatibleFrom(), expected, List.copyOf(applied));
    }
}
