package io.github.khopland.pgtasks.postgres;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Takes a release's snapshot for {@link SchemaUpgradeTest}: copies this build's schema files and
 * the rows {@link UpgradeSeed} leaves behind into {@code src/test/resources/.../released/<version>/}.
 * Run it on the release's commit, once, and commit what it writes (RELEASING.md):
 * <pre>
 * mvn test -pl pgtasks-postgres -am -Dtest=SchemaSnapshot -Dsurefire.failIfNoSpecifiedTests=false -Dpgtasks.schema.snapshot=0.1.0
 * </pre>
 * Skipped without {@code pgtasks.schema.snapshot}.
 */
@EnabledIfSystemProperty(named = "pgtasks.schema.snapshot", matches = ".+")
class SchemaSnapshot {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));

    @Test
    void snapshot(PostgresDatabase database) throws Exception {
        String version = System.getProperty("pgtasks.schema.snapshot");
        Path directory = Path.of("src/test/resources", SchemaReleases.RESOURCES, version);
        assertThat(directory).as("snapshots are never retaken: a release's files and rows are what it shipped").doesNotExist();
        Files.createDirectories(directory);

        for (String file : TaskSchema.FILES) {
            try (InputStream input = TaskSchema.class.getResourceAsStream("db/" + file)) {
                assertThat(input).as(file).isNotNull();
                Files.write(directory.resolve(file), input.readAllBytes());
            }
        }
        try (PostgresStoreFixture fixture = new PostgresStoreFixture(database)) {
            UpgradeSeed.seed(fixture);
        }
        String header = "-- The rows pgtasks " + version + " left in every state (UpgradeSeed), for SchemaUpgradeTest.\n"
            + "-- Written by SchemaSnapshot; never edit it.\n";
        Files.writeString(directory.resolve(SchemaReleases.SEED), SchemaReleases.dump(database, TaskSchema.DEFAULT_SCHEMA, header),
            StandardCharsets.UTF_8);
    }
}
