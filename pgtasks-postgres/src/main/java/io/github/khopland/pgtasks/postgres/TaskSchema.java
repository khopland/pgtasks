package io.github.khopland.pgtasks.postgres;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * The pgtasks schema, named {@value #DEFAULT_SCHEMA} unless configured otherwise (see
 * {@link #validName}). It ships as plain SQL files under {@value #LOCATION}, so the library
 * depends on no migration tool and every consumer picks how to apply them. The files qualify every
 * name with the placeholder {@code ${pgtasks_schema}} ({@value #PLACEHOLDER}), which each path fills in:
 *
 * <ul>
 *   <li>nothing in particular: the built-in runner applies them at startup ({@link SchemaMode#MIGRATE},
 *       the default, or {@link #migrate} directly);</li>
 *   <li>Flyway: a separate Flyway instance, so the version numbers don't clash with the
 *       application's own migrations, with the name as a placeholder (also for the default name),
 *       and {@link SchemaMode#VALIDATE}:
 *       <pre>{@code
 * Flyway.configure()
 *     .dataSource(dataSource)
 *     .locations("classpath:io/github/khopland/pgtasks/postgres/db")
 *     .schemas("pgtasks")
 *     .placeholders(Map.of("pgtasks_schema", "pgtasks"))
 *     .table("flyway_task_history")
 *     .load()
 *     .migrate();
 *       }</pre></li>
 *   <li>Liquibase: include {@value #LIQUIBASE_CHANGELOG} from the application's changelog. It is a
 *       plain XML resource that wraps the same files with {@code sqlFile} ({@code runOnChange} for
 *       the repeatable ones), so it adds no dependency. The changelog parameter {@value #PLACEHOLDER}
 *       names the schema and defaults to {@value #DEFAULT_SCHEMA}. Then {@link SchemaMode#VALIDATE};</li>
 *   <li>a DBA pipeline: the files in order, with the placeholder replaced, as the schema owner, and
 *       {@link SchemaMode#VALIDATE}.</li>
 * </ul>
 *
 * <p>Conventions every file follows, so all four paths behave the same:
 * {@code V<version>__<description>.sql} for tables, types and indexes, applied once, each ending
 * by recording its version and the lowest version it stays compatible with in {@code pgtasks.meta};
 * {@code R__<description>.sql} for functions, views and triggers, idempotent and re-applied when
 * they change, with a {@code -- revision: <n>} header comment raised by every change, so the
 * built-in runner never applies an older definition over a newer one during a rolling deploy
 * ({@code SchemaFreezeTest} pins each revision's checksum); and a {@value SchemaScript#NO_TRANSACTION} header comment for scripts that must
 * run outside a transaction, such as {@code CREATE INDEX CONCURRENTLY}.
 *
 * <p>Rights: with {@code MIGRATE} the application's user owns the schema. Otherwise the runtime
 * user needs USAGE on the schema, SELECT, INSERT, UPDATE and DELETE on its tables, USAGE on
 * {@code task_id_seq}, and EXECUTE on {@code create_topic}. No DDL rights: a topic is a row.
 */
public final class TaskSchema {

    /** The schema's name unless configured otherwise. */
    public static final String DEFAULT_SCHEMA = "pgtasks";

    /**
     * The placeholder the SQL files write as {@code ${pgtasks_schema}}: a Flyway placeholder and a
     * Liquibase changelog parameter of this name fill it in.
     */
    public static final String PLACEHOLDER = "pgtasks_schema";

    /**
     * At most 50 characters, so the notification channels named after the schema, such as
     * {@code <schema>_ready}, stay within Postgres' 63.
     */
    private static final Pattern NAME = Pattern.compile("[a-z_][a-z0-9_]{0,49}");

    /** {@code pgtasks.} where it starts a qualified name in the store's SQL. */
    private static final Pattern QUALIFIED = Pattern.compile("(?<![\\w.\"$])pgtasks\\.(?=[a-z_])");

    public static final String LOCATION = "io/github/khopland/pgtasks/postgres/db";

    public static final String LIQUIBASE_CHANGELOG = LOCATION + "/changelog.xml";

    /** The files, in the order a DBA pipeline runs them: versioned by version, then repeatable by name. */
    static final List<String> FILES = List.of(
        "V001__tasks_schema.sql",
        "R__create_topic.sql",
        "R__notify_ready.sql",
        "R__notify_topic.sql",
        "R__task_view.sql"
    );

    /** The schema version this library is built for. */
    public static final int VERSION = 1;

    /**
     * The oldest Postgres major version the store runs on. Its statements are written against the
     * planners of 17 and later, and CI checks their plans on each of them: on 16 and earlier a
     * SUPERSEDE or REPLACE push can read its topic's whole queue.
     */
    public static final int MIN_POSTGRES_VERSION = 17;

    /** What the schema's {@code meta} table records. */
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

    /**
     * Checks a schema name: a plain lowercase identifier ({@code [a-z_][a-z0-9_]*}) of at most 50
     * characters that doesn't start with {@code pg_}, which Postgres reserves. It is written
     * unquoted into every statement, so the runner also refuses an SQL reserved word, such as
     * {@code user}, when it migrates or validates.
     *
     * @return the name
     * @throws IllegalArgumentException if it isn't one
     */
    public static String validName(String schema) {
        if (schema == null || !NAME.matcher(schema).matches() || schema.startsWith("pg_")) {
            throw new IllegalArgumentException("Not a valid schema name for pgtasks: " + (schema == null ? null : "'" + schema + "'")
                + ". Use a plain lowercase identifier ([a-z_][a-z0-9_]*) of at most 50 characters that doesn't start with pg_.");
        }
        return schema;
    }

    /** The longest name Postgres keeps for a channel; a longer one is cut when listened to, and refused when notified. */
    private static final int MAX_CHANNEL = 63;

    /**
     * A channel's name in the database: {@code <schema>_<channel>}, so two schemas in one database
     * don't hear each other. Postgres keeps at most 63 bytes of it: a longer one would be listened
     * to cut short, and refused by pg_notify, so the channel would never deliver anything.
     *
     * @throws IllegalArgumentException if the name is longer than Postgres keeps
     */
    static String channel(String schema, String channel) {
        String name = schema + "_" + channel;
        if (name.length() > MAX_CHANNEL) {
            throw new IllegalArgumentException("Channel '" + channel + "' of schema " + schema + " is named " + name + " in the database, "
                + "longer than the " + MAX_CHANNEL + " characters Postgres keeps: shorten the channel or the schema's name");
        }
        return name;
    }

    /** Applies whatever is missing to the {@value #DEFAULT_SCHEMA} schema with the built-in runner, then checks the version. */
    public static Status migrate(DataSource dataSource) throws SQLException {
        return migrate(dataSource, DEFAULT_SCHEMA);
    }

    /**
     * Applies whatever is missing to the given schema with the built-in runner, then checks the
     * version. Each statement waits at most 5 seconds for a lock; see {@link #migrate(DataSource, String, Duration)}.
     */
    public static Status migrate(DataSource dataSource, String schema) throws SQLException {
        return migrate(dataSource, schema, SchemaMigrator.LOCK_TIMEOUT);
    }

    /**
     * Applies whatever is missing to the given schema with the built-in runner, then checks the
     * version. Changing a trigger or table needs a lock that waits behind every open transaction
     * that touched the table, and every push and claim then queues behind the waiting change. So
     * each statement waits at most {@code lockTimeout} for a lock; on a timeout the runner rolls the
     * file back and tries again a few times with a growing pause, then fails with a
     * {@link TaskSchemaException} naming the transactions that hold locks on the schema's tables.
     */
    public static Status migrate(DataSource dataSource, String schema, Duration lockTimeout) throws SQLException {
        validName(schema);
        try (Connection connection = dataSource.getConnection()) {
            refuseOldServer(connection);
            refuseKeyword(connection, schema);
            return new SchemaMigrator(scripts(), schema, lockTimeout, SchemaMigrator.ATTEMPTS, SchemaMigrator.BACKOFF).migrate(connection);
        }
    }

    /** Only checks the {@value #DEFAULT_SCHEMA} schema's meta table; for schemas that another tool applies. */
    public static Status validate(DataSource dataSource) throws SQLException {
        return validate(dataSource, DEFAULT_SCHEMA);
    }

    /** Only checks the given schema's meta table; for schemas that another tool applies. */
    public static Status validate(DataSource dataSource, String schema) throws SQLException {
        validName(schema);
        try (Connection connection = dataSource.getConnection()) {
            refuseOldServer(connection);
            refuseKeyword(connection, schema);
            return check(schema, readMeta(connection, schema), VERSION, List.of());
        }
    }

    public static Optional<Status> prepare(DataSource dataSource, SchemaMode mode) throws SQLException {
        return prepare(dataSource, mode, DEFAULT_SCHEMA);
    }

    public static Optional<Status> prepare(DataSource dataSource, SchemaMode mode, String schema) throws SQLException {
        return switch (mode) {
            case MIGRATE -> Optional.of(migrate(dataSource, schema));
            case VALIDATE -> Optional.of(validate(dataSource, schema));
            case NONE -> Optional.empty();
        };
    }

    /** A schema file's content for the given schema: what Flyway and Liquibase make of it, and what a DBA pipeline runs. */
    static String render(String content, String schema) {
        return content.replace("${" + PLACEHOLDER + "}", schema);
    }

    /** The store's SQL, written for the {@value #DEFAULT_SCHEMA} schema, for the given one. */
    static String qualify(String sql, String schema) {
        return schema.equals(DEFAULT_SCHEMA) ? sql : QUALIFIED.matcher(sql).replaceAll(schema + ".");
    }

    /** Before anything is applied, so an older server is left as it was. */
    private static void refuseOldServer(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT current_setting('server_version_num')::integer, current_setting('server_version')")) {
            resultSet.next();
            checkServerVersion(resultSet.getInt(1), resultSet.getString(2));
        }
    }

    /**
     * @param versionNumber the server's {@code server_version_num}, such as 170012 for 17.12
     * @param version       its {@code server_version}, for the message
     */
    static void checkServerVersion(int versionNumber, String version) {
        if (versionNumber / 10_000 < MIN_POSTGRES_VERSION) {
            throw new TaskSchemaException("This database runs Postgres " + version + ", but pgtasks needs Postgres " + MIN_POSTGRES_VERSION
                + " or later: its statements are planned for those versions. Upgrade the server.");
        }
    }

    /** Reserved words would need quoting wherever the name appears; types and functions can't be qualified with some others. */
    private static void refuseKeyword(Connection connection, String schema) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT catdesc FROM pg_get_keywords() WHERE word = ? AND catcode IN ('R', 'T', 'C')")) {
            statement.setString(1, schema);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    throw new IllegalArgumentException("'" + schema + "' can't name the pgtasks schema: it is an SQL keyword ("
                        + resultSet.getString(1) + "). Pick another name.");
                }
            }
        }
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

    static Optional<Meta> readMeta(Connection connection, String schema) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet exists = statement.executeQuery("SELECT to_regclass('" + schema + ".meta') IS NOT NULL")) {
                exists.next();
                if (!exists.getBoolean(1)) {
                    return Optional.empty();
                }
            }
            try (ResultSet resultSet = statement.executeQuery("SELECT version, compatible_from FROM " + schema + ".meta")) {
                return resultSet.next() ? Optional.of(new Meta(resultSet.getInt(1), resultSet.getInt(2))) : Optional.empty();
            }
        }
    }

    /**
     * The one version check every path shares. The schema must be at the expected version, or
     * one version ahead if it marks itself compatible, so old and new nodes can run side by side
     * during a rolling deploy.
     */
    static Status check(String schema, Optional<Meta> found, int expected, List<String> applied) {
        if (found.isEmpty()) {
            throw new TaskSchemaException("The " + schema + " schema doesn't exist in this database yet (there is no " + schema + ".meta). Either "
                + "let the library create it at startup with schema mode MIGRATE, or apply the SQL files in classpath:" + LOCATION
                + " with Flyway, Liquibase or your DBA pipeline before starting, and keep schema mode VALIDATE.");
        }
        Meta meta = found.get();
        if (meta.version() < expected) {
            throw new TaskSchemaException("The " + schema + " schema is at version " + meta.version() + ", but this library needs version " + expected
                + ". Apply the newer files from classpath:" + LOCATION + ": with schema mode MIGRATE the library does it at startup; "
                + "with Flyway, Liquibase or a DBA pipeline, run them before deploying this version.");
        }
        if (meta.version() > expected + 1) {
            throw new TaskSchemaException("The " + schema + " schema is at version " + meta.version() + ", more than one version ahead of this library ("
                + expected + "). A rolling deploy may only run one schema version ahead; upgrade this application.");
        }
        if (meta.compatibleFrom() > expected) {
            throw new TaskSchemaException("The " + schema + " schema is at version " + meta.version() + " and only compatible with library schema version "
                + meta.compatibleFrom() + " and later, but this library uses version " + expected
                + ". A newer release migrated it; upgrade this application or restore the schema.");
        }
        return new Status(meta.version(), meta.compatibleFrom(), expected, List.copyOf(applied));
    }
}
