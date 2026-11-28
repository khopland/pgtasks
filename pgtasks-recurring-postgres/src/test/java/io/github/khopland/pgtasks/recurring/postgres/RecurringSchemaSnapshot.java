package io.github.khopland.pgtasks.recurring.postgres;

import java.nio.file.Path;

import io.github.khopland.pgtasks.SchemaMode;
import io.github.khopland.pgtasks.postgres.ExtensionSchemaReleases;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.postgres.TaskSchema;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Takes the recurring tasks' snapshot for {@link RecurringSchemaUpgradeTest}, with the engine's
 * ({@code SchemaSnapshot}); the release workflow runs every {@code *SchemaSnapshot}. Skipped
 * without {@code pgtasks.schema.snapshot}.
 */
@EnabledIfSystemProperty(named = "pgtasks.schema.snapshot", matches = ".+")
class RecurringSchemaSnapshot {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));

    @Test
    void snapshot(PostgresDatabase database) throws Exception {
        String version = System.getProperty("pgtasks.schema.snapshot");
        ExtensionSchemaReleases releases = RecurringSchemaUpgradeTest.RELEASES;
        PostgresScheduleStore.SCHEMA.migrate(database.dataSource(), TaskSchema.DEFAULT_SCHEMA);
        RecurringSchemaUpgradeTest.UpgradeSeedRows.seed(new PostgresTaskStore(database.dataSource(), SchemaMode.MIGRATE));
        releases.snapshot(Path.of("src/test/resources", releases.resources(), version), database.dataSource(), TaskSchema.DEFAULT_SCHEMA,
            "-- The recurring tasks' rows at pgtasks " + version + ", for RecurringSchemaUpgradeTest.\n"
                + "-- Written by RecurringSchemaSnapshot; never edit it.\n");
    }
}
