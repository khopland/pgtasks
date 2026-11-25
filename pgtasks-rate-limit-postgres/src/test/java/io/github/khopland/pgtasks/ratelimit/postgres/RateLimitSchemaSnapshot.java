package io.github.khopland.pgtasks.ratelimit.postgres;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import io.github.khopland.pgtasks.SchemaMode;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.postgres.TaskSchema;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Takes the cluster rate limit's snapshot for {@link RateLimitSchemaUpgradeTest}, with the engine's
 * ({@code SchemaSnapshot}); RELEASING.md runs both. Skipped without {@code pgtasks.schema.snapshot}.
 */
@EnabledIfSystemProperty(named = "pgtasks.schema.snapshot", matches = ".+")
class RateLimitSchemaSnapshot {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));

    @Test
    void snapshot(PostgresDatabase database) throws Exception {
        String version = System.getProperty("pgtasks.schema.snapshot");
        Path directory = Path.of("src/test/resources", RateLimitSchemaReleases.RESOURCES, version);
        assertThat(directory).as("snapshots are never retaken: a release's files and rows are what it shipped").doesNotExist();
        Files.createDirectories(directory);
        for (String file : RateLimitSchemaReleases.shippedFiles()) {
            try (InputStream input = PostgresSlidingWindowStore.class.getResourceAsStream("db/" + file)) {
                assertThat(input).as(file).isNotNull();
                Files.write(directory.resolve(file), input.readAllBytes());
            }
        }
        RateLimitSchemaUpgradeTest.UpgradeSeedRows.seed(
            new PostgresSlidingWindowStore(new PostgresTaskStore(database.dataSource(), SchemaMode.MIGRATE)));
        String header = "-- The cluster rate limit's rows at pgtasks " + version + ", for RateLimitSchemaUpgradeTest.\n"
            + "-- Written by RateLimitSchemaSnapshot; never edit it.\n";
        Files.writeString(directory.resolve(RateLimitSchemaReleases.SEED), RateLimitSchemaReleases.dump(database, TaskSchema.DEFAULT_SCHEMA, header));
    }
}
