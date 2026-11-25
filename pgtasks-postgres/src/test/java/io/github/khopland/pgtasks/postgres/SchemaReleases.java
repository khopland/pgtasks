package io.github.khopland.pgtasks.postgres;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import io.github.khopland.pgtasks.test.PostgresDatabase;

/**
 * The schema files and seed rows of each release, kept in
 * {@code src/test/resources/io/github/khopland/pgtasks/postgres/released/<version>/} by
 * {@link SchemaSnapshot}. The tables are dumped as JSON rows and loaded back with
 * {@code json_populate_recordset}, so a seed needs nothing but the schema it was taken from.
 */
final class SchemaReleases {

    static final String RESOURCES = "io/github/khopland/pgtasks/postgres/released";
    static final String SEED = "seed.sql";
    static final String PLACEHOLDER = "${pgtasks_schema}";

    /** The tables a seed holds, in the order their foreign keys need. */
    static final List<String> TABLES = List.of("topic", "node", "task", "queue", "topic_count", "lease");

    private SchemaReleases() {
    }

    /** Every release with a snapshot, oldest first. */
    static List<String> versions() {
        URL root = SchemaReleases.class.getClassLoader().getResource(RESOURCES);
        if (root == null) {
            return List.of();
        }
        try (Stream<Path> releases = Files.list(Path.of(root.toURI()))) {
            return releases.filter(Files::isDirectory).map(path -> path.getFileName().toString())
                .sorted(Comparator.comparing(SchemaReleases::versionKey)).toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The release's versioned and repeatable files, as its runner applied them. */
    static List<SchemaScript> scripts(String version) {
        try (Stream<Path> files = Files.list(directory(version))) {
            return files.filter(path -> !path.getFileName().toString().equals(SEED) && path.toString().endsWith(".sql"))
                .sorted().map(path -> SchemaScript.of(path.getFileName().toString(), read(path))).toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String seed(String version) {
        return read(directory(version).resolve(SEED));
    }

    /** The rows of {@link #TABLES} in {@code schema}, as SQL that loads them into the schema the placeholder names. */
    static String dump(PostgresDatabase database, String schema, String header) {
        StringBuilder sql = new StringBuilder(header);
        for (String table : TABLES) {
            List<List<Object>> rows = database.rows("SELECT row_to_json(t)::text FROM " + schema + "." + table + " t ORDER BY 1");
            if (rows.isEmpty()) {
                continue;
            }
            sql.append("\nINSERT INTO ").append(PLACEHOLDER).append('.').append(table)
                .append(" OVERRIDING SYSTEM VALUE\nSELECT * FROM json_populate_recordset(NULL::").append(PLACEHOLDER).append('.').append(table)
                .append(", $seed$[\n");
            for (int index = 0; index < rows.size(); index++) {
                sql.append(rows.get(index).getFirst()).append(index + 1 < rows.size() ? ",\n" : "\n");
            }
            sql.append("]$seed$);\n");
        }
        // Ids pushed after the upgrade continue after the seed's.
        sql.append("""

            SELECT setval(pg_get_serial_sequence('${pgtasks_schema}.topic', 'id'), coalesce(max(id), 0) + 1, false) FROM ${pgtasks_schema}.topic;
            SELECT setval(pg_get_serial_sequence('${pgtasks_schema}.node', 'id'), coalesce(max(id), 0) + 1, false) FROM ${pgtasks_schema}.node;
            SELECT setval('${pgtasks_schema}.task_id_seq', coalesce(max(id), 0) + 1, false) FROM ${pgtasks_schema}.task;
            """);
        return sql.toString();
    }

    static void load(PostgresDatabase database, String seed, String schema) {
        database.execute(seed.replace(PLACEHOLDER, schema));
    }

    private static Path directory(String version) {
        URL url = SchemaReleases.class.getClassLoader().getResource(RESOURCES + "/" + version);
        if (url == null) {
            throw new IllegalArgumentException("No schema snapshot for " + version);
        }
        try {
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String read(Path path) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 0.10.0 after 0.9.0. */
    private static String versionKey(String version) {
        StringBuilder key = new StringBuilder();
        for (String part : version.split("[.-]")) {
            key.append(part.chars().allMatch(Character::isDigit) ? String.format("%010d", Long.parseLong(part)) : part).append('.');
        }
        return key.toString();
    }
}
