package io.github.khopland.pgtasks.postgres;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import javax.sql.DataSource;

/**
 * An extension's schema as each release shipped it, for the extension's upgrade test: a
 * snapshot per release on the test class path, under {@code <resources>/<version>/}, holding the
 * SQL files as they shipped and a {@value #SEED} of the rows that release's code left in its
 * tables. An upgrade test applies a release's files, loads its rows, applies the current files
 * over them, and checks that the current code reads them; the engine's own schema is tested the
 * same way.
 *
 * <pre>{@code
 * static final ExtensionSchemaReleases RELEASES = ExtensionSchemaReleases.of(PostgresWindowStore.SCHEMA, "io/example/windows/released");
 *
 * RELEASES.schema("0.2.0").migrate(dataSource, "pgtasks");
 * database.execute(RELEASES.seed("0.2.0", "pgtasks"));
 * PostgresWindowStore.SCHEMA.migrate(dataSource, "pgtasks");
 * }</pre>
 *
 * A release's snapshot is taken once, when it ships, with {@link #snapshot}, and never again: the
 * release workflow runs every test class whose name ends in {@code SchemaSnapshot}.
 */
public final class ExtensionSchemaReleases {

    /** The file of a snapshot that holds its rows. */
    public static final String SEED = "seed.sql";
    /** What a seed has in place of the schema's name. */
    public static final String PLACEHOLDER = "${pgtasks_schema}";

    private final ExtensionSchema schema;
    private final String resources;

    private ExtensionSchemaReleases(ExtensionSchema schema, String resources) {
        this.schema = schema;
        this.resources = resources;
    }

    /**
     * @param schema    the extension's schema as it is now
     * @param resources the class path directory of the snapshots, such as
     *                  {@code io/example/windows/released}, without a leading or trailing slash
     */
    public static ExtensionSchemaReleases of(ExtensionSchema schema, String resources) {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(resources, "resources");
        String stripped = resources.replace('\\', '/').replaceAll("^/+|/+$", "");
        if (stripped.isEmpty()) {
            throw new IllegalArgumentException("The snapshots need a class path directory");
        }
        return new ExtensionSchemaReleases(schema, stripped);
    }

    /** The class path directory of the snapshots. */
    public String resources() {
        return resources;
    }

    /** Every release with a snapshot, oldest first; none before the first release. */
    public List<String> versions() {
        URL root = schema.owner().getClassLoader().getResource(resources);
        if (root == null) {
            return List.of();
        }
        try (Stream<Path> releases = Files.list(Path.of(root.toURI()))) {
            return releases.filter(Files::isDirectory).map(path -> path.getFileName().toString())
                .sorted(Comparator.comparing(ExtensionSchemaReleases::versionKey)).toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The schema as the release shipped it, read from its snapshot: the versioned files in order, then the repeatable ones. */
    public ExtensionSchema schema(String version) {
        try (Stream<Path> files = Files.list(directory(version))) {
            List<String> names = files.map(path -> path.getFileName().toString())
                .filter(name -> name.endsWith(".sql") && !name.equals(SEED))
                .sorted(Comparator.comparing((String name) -> !name.startsWith("V")).thenComparing(ExtensionSchemaReleases::fileKey))
                .toList();
            return ExtensionSchema.of(schema.name(), schema.owner(), resources + "/" + version, names, schema.tables());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The rows the release left, as SQL that loads them into this schema. */
    public String seed(String version, String into) {
        TaskSchema.validName(into);
        try {
            return Files.readString(directory(version).resolve(SEED)).replace(PLACEHOLDER, into);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes a release's snapshot into {@code directory}: the current files as the jar has them,
     * and the rows in the schema's tables now, which the test has written with the release's code.
     *
     * @throws IllegalStateException if the directory exists: a release's snapshot is taken once
     */
    public void snapshot(Path directory, DataSource dataSource, String from, String header) throws IOException, SQLException {
        if (Files.exists(directory)) {
            throw new IllegalStateException(directory + " exists: snapshots are never retaken, a release's files and rows are what it shipped");
        }
        Files.createDirectories(directory);
        for (String file : schema.files()) {
            try (InputStream input = schema.owner().getResourceAsStream("/" + schema.location() + "/" + file)) {
                if (input == null) {
                    throw new IllegalStateException("Missing extension schema file on the classpath: " + schema.location() + "/" + file);
                }
                Files.write(directory.resolve(file), input.readAllBytes());
            }
        }
        Files.writeString(directory.resolve(SEED), dump(dataSource, from, header));
    }

    /**
     * The rows in the schema's tables, as SQL that loads them into whichever schema the
     * {@value #PLACEHOLDER} placeholder names: JSON rows, loaded with {@code json_populate_recordset},
     * so a seed needs nothing but the schema it was taken from.
     */
    public String dump(DataSource dataSource, String from, String header) throws SQLException {
        String checked = TaskSchema.validName(from);
        StringBuilder sql = new StringBuilder(header);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            for (String table : schema.tables()) {
                List<String> rows = new ArrayList<>();
                try (ResultSet result = statement.executeQuery("SELECT row_to_json(t)::text FROM " + checked + "." + table + " t ORDER BY 1")) {
                    while (result.next()) {
                        rows.add(result.getString(1));
                    }
                }
                sql.append("INSERT INTO ").append(PLACEHOLDER).append('.').append(table)
                    .append("\nSELECT * FROM json_populate_recordset(NULL::").append(PLACEHOLDER).append('.').append(table).append(", $seed$[\n");
                for (int index = 0; index < rows.size(); index++) {
                    sql.append(rows.get(index)).append(index + 1 < rows.size() ? ",\n" : "\n");
                }
                sql.append("]$seed$);\n");
            }
        }
        return sql.toString();
    }

    private Path directory(String version) {
        URL url = schema.owner().getClassLoader().getResource(resources + "/" + version);
        if (url == null) {
            throw new IllegalArgumentException("No " + schema.name() + " schema snapshot for " + version + " under " + resources);
        }
        try {
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    /** 0.10.0 after 0.9.0: each part compared as a number. */
    private static String versionKey(String version) {
        StringBuilder key = new StringBuilder();
        for (String part : version.split("[.-]", -1)) {
            key.append(!part.isEmpty() && part.chars().allMatch(Character::isDigit) ? String.format("%010d", Long.parseLong(part)) : part).append('.');
        }
        return key.toString();
    }

    /** V2 before V10. */
    private static String fileKey(String file) {
        int split = file.indexOf("__");
        String version = split > 1 && file.startsWith("V") ? file.substring(1, split) : "";
        return version.chars().allMatch(Character::isDigit) && !version.isEmpty() ? String.format("%010d", Long.parseLong(version)) + file : file;
    }

    @Override
    public String toString() {
        return "ExtensionSchemaReleases[" + schema.name() + ", " + resources + "]";
    }
}
