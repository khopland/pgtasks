package io.github.khopland.pgtasks.postgres;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * The SQL files an extension owns, separate from the engine's schema. The built-in runner applies
 * them once each under an advisory lock and keeps their history in
 * {@code <schema>.<name>_schema_history}, so extensions can't clash over version numbers or
 * checksums. Versioned files are named {@code V<version>__<what>.sql} and never change after being
 * applied; repeatable {@code R__} files may.
 *
 * <p>An extension's files use the same {@code ${pgtasks_schema}} placeholder and the same schema
 * modes as the engine's: {@link #apply(PostgresTaskStore)} migrates them with the built-in runner
 * in {@code MIGRATE}, checks their tables in {@code VALIDATE}, and leaves them alone in {@code NONE}.
 * Applications that run Flyway, Liquibase or a DBA pipeline can apply the same files and set
 * {@code VALIDATE}; {@link #describe()} names their classpath location.
 */
public final class ExtensionSchema {

    private final String historyTable;
    private final Class<?> owner;
    private final String location;
    private final List<String> files;
    private final List<String> tables;

    private ExtensionSchema(String historyTable, Class<?> owner, String location, List<String> files, List<String> tables) {
        this.historyTable = historyTable;
        this.owner = owner;
        this.location = location;
        this.files = files;
        this.tables = tables;
    }

    /**
     * @param name     a short lowercase name for the extension, such as {@code rate_limit}; its
     *                 history table is {@code <name>_schema_history}
     * @param owner    a class in the extension's jar, to read its resources with
     * @param location the classpath directory of the SQL files, without a leading or trailing slash
     * @param files    the files in the order a migration tool runs them, named the Flyway way,
     *                 with at least one versioned file
     * @param tables   the tables {@link #validate} expects to find once the files have been applied
     */
    public static ExtensionSchema of(String name, Class<?> owner, String location, List<String> files, List<String> tables) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(files, "files");
        Objects.requireNonNull(tables, "tables");
        String history = Objects.requireNonNull(name, "name") + "_schema_history";
        try {
            TaskSchema.validName(history);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Not a valid extension name: " + name + ". Use a plain lowercase identifier "
                + "([a-z_][a-z0-9_]*) that doesn't start with pg_.", e);
        }
        if (files.isEmpty()) {
            throw new IllegalArgumentException("An extension schema needs at least one file");
        }
        for (String file : files) {
            SchemaScript.of(file, ""); // checks the name, before anything touches the database
        }
        if (files.stream().noneMatch(file -> file.startsWith("V"))) {
            throw new IllegalArgumentException("An extension schema needs at least one versioned V<version>__<what>.sql file");
        }
        for (String table : tables) {
            TaskSchema.validName(table);
        }
        return new ExtensionSchema(history, owner, strip(location), List.copyOf(files), List.copyOf(tables));
    }

    /** Applies or checks the files according to the store's schema mode; extensions call this before using their tables. */
    public void apply(PostgresTaskStore store) throws SQLException {
        Objects.requireNonNull(store, "store");
        switch (store.mode()) {
            case MIGRATE -> migrate(store.dataSource(), store.schema());
            case VALIDATE -> validate(store.dataSource(), store.schema());
            case NONE -> { /* The application owns both schemas; don't even check. */ }
        }
    }

    /** Applies any files that aren't applied yet and returns the version they are at. */
    public int migrate(DataSource dataSource, String schema) throws SQLException {
        Objects.requireNonNull(dataSource, "dataSource");
        String checked = TaskSchema.validName(schema);
        try (Connection connection = dataSource.getConnection()) {
            TaskSchema.refuseOldServer(connection);
            TaskSchema.refuseKeyword(connection, checked);
            return new SchemaMigrator(scripts(), checked, historyTable).migrate(connection).version();
        }
    }

    /** Checks that an application using schema mode {@code VALIDATE} has applied the files already. */
    public void validate(DataSource dataSource, String schema) throws SQLException {
        Objects.requireNonNull(dataSource, "dataSource");
        String checked = TaskSchema.validName(schema);
        try (Connection connection = dataSource.getConnection()) {
            TaskSchema.refuseOldServer(connection);
            TaskSchema.refuseKeyword(connection, checked);
            for (String table : tables) {
                try (PreparedStatement statement = connection.prepareStatement("SELECT to_regclass(?)")) {
                    statement.setString(1, checked + "." + table);
                    try (ResultSet resultSet = statement.executeQuery()) {
                        resultSet.next();
                        if (resultSet.getString(1) == null) {
                            throw new TaskSchemaException("The " + checked + "." + table + " table is missing. Apply " + describe()
                                + " before starting with schema mode VALIDATE, or let the built-in runner apply them with MIGRATE.");
                        }
                    }
                }
            }
        }
    }

    /** The classpath location and files, for error messages and migration-tool configuration. */
    public String describe() {
        return String.join(", ", files.stream().map(file -> "classpath:" + location + "/" + file).toList());
    }

    private List<SchemaScript> scripts() {
        return files.stream().map(file -> {
            String path = location + "/" + file;
            try (InputStream input = owner.getResourceAsStream("/" + path)) {
                if (input == null) {
                    throw new IllegalStateException("Missing extension schema file on the classpath: " + path);
                }
                return SchemaScript.of(file, new String(input.readAllBytes(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).toList();
    }

    private static String strip(String location) {
        Objects.requireNonNull(location, "location");
        String stripped = location.replace('\\', '/').replaceAll("^/+|/+$", "");
        if (stripped.isEmpty()) {
            throw new IllegalArgumentException("An extension schema needs a classpath directory");
        }
        return stripped;
    }
}
