package io.github.khopland.pgtasks.ratelimit.postgres;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import io.github.khopland.pgtasks.postgres.ExtensionSchema;
import io.github.khopland.pgtasks.test.PostgresDatabase;

/**
 * The cluster rate limit's schema files and seed rows at each release, in
 * {@code src/test/resources/io/github/khopland/pgtasks/ratelimit/postgres/released/<version>/},
 * written by {@link RateLimitSchemaSnapshot}. The engine's own schema has the same in
 * pgtasks-postgres ({@code SchemaReleases}).
 */
final class RateLimitSchemaReleases {

    static final String RESOURCES = "io/github/khopland/pgtasks/ratelimit/postgres/released";
    static final String SEED = "seed.sql";
    static final String PLACEHOLDER = "${pgtasks_schema}";
    static final String TABLE = "rate_limit_window";

    private RateLimitSchemaReleases() {
    }

    /** Every release with a snapshot. Each is upgraded from on its own, so their order doesn't matter. */
    static List<String> versions() {
        URL root = RateLimitSchemaReleases.class.getClassLoader().getResource(RESOURCES);
        if (root == null) {
            return List.of();
        }
        try (Stream<Path> releases = Files.list(Path.of(root.toURI()))) {
            return releases.filter(Files::isDirectory).map(path -> path.getFileName().toString()).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The release's schema, read from its snapshot like the extension reads its own from the jar. */
    static ExtensionSchema schema(String version) {
        Path directory = directory(version);
        try (Stream<Path> files = Files.list(directory)) {
            List<String> names = files.map(path -> path.getFileName().toString())
                .filter(name -> name.endsWith(".sql") && !name.equals(SEED)).sorted().toList();
            return ExtensionSchema.of("rate_limit", RateLimitSchemaReleases.class, RESOURCES + "/" + version, names, List.of(TABLE));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String seed(String version) {
        try {
            return Files.readString(directory(version).resolve(SEED));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The shipped files' names, from the extension's own description of them. */
    static List<String> shippedFiles() {
        return Arrays.stream(PostgresSlidingWindowStore.SCHEMA.describe().split(", "))
            .map(entry -> entry.substring(entry.lastIndexOf('/') + 1)).toList();
    }

    /** The counter rows in {@code schema}, as SQL that loads them into the schema the placeholder names. */
    static String dump(PostgresDatabase database, String schema, String header) {
        List<List<Object>> rows = database.rows("SELECT row_to_json(t)::text FROM " + schema + "." + TABLE + " t ORDER BY 1");
        StringBuilder sql = new StringBuilder(header);
        sql.append("INSERT INTO ").append(PLACEHOLDER).append('.').append(TABLE)
            .append("\nSELECT * FROM json_populate_recordset(NULL::").append(PLACEHOLDER).append('.').append(TABLE).append(", $seed$[\n");
        for (int index = 0; index < rows.size(); index++) {
            sql.append(rows.get(index).getFirst()).append(index + 1 < rows.size() ? ",\n" : "\n");
        }
        return sql.append("]$seed$);\n").toString();
    }

    private static Path directory(String version) {
        URL url = RateLimitSchemaReleases.class.getClassLoader().getResource(RESOURCES + "/" + version);
        if (url == null) {
            throw new IllegalArgumentException("No rate limit schema snapshot for " + version);
        }
        try {
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }
}
