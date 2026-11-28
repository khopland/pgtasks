package io.github.khopland.pgtasks.pause.postgres;

import java.util.List;
import java.util.stream.Stream;

import io.github.khopland.pgtasks.SchemaMode;
import io.github.khopland.pgtasks.pause.PauseStore;
import io.github.khopland.pgtasks.postgres.ExtensionSchema;
import io.github.khopland.pgtasks.postgres.ExtensionSchemaReleases;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.postgres.TaskSchema;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Upgrading the pause extension's own table from every release keeps its pauses. Each release's
 * files are applied with the extension's runner, its rows loaded, the current files applied over
 * them, and the pauses read and changed by the current store. Until the first release, the
 * snapshot is taken of the current files in a scratch schema, as in {@code SchemaUpgradeTest} for
 * the engine's schema.
 */
class PauseSchemaUpgradeTest {

    static final String UNRELEASED = "unreleased";
    static final ExtensionSchemaReleases RELEASES =
        ExtensionSchemaReleases.of(PostgresPauseStore.SCHEMA, "io/github/khopland/pgtasks/pause/postgres/released");

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));

    static Stream<String> releases() {
        List<String> versions = RELEASES.versions();
        return versions.isEmpty() ? Stream.of(UNRELEASED) : versions.stream();
    }

    @ParameterizedTest(name = "from {0}")
    @MethodSource("releases")
    void pauses_survive_the_upgrade(String release, PostgresDatabase database) throws Exception {
        ExtensionSchema released;
        String seed;
        if (release.equals(UNRELEASED)) {
            released = PostgresPauseStore.SCHEMA;
            seed = seedFromCurrentCode(database);
        } else {
            released = RELEASES.schema(release);
            seed = RELEASES.seed(release, TaskSchema.DEFAULT_SCHEMA);
        }
        released.migrate(database.dataSource(), TaskSchema.DEFAULT_SCHEMA);
        database.execute(seed);

        PostgresPauseStore.SCHEMA.migrate(database.dataSource(), TaskSchema.DEFAULT_SCHEMA);
        assertThat(database.count("SELECT count(*) FROM pgtasks.pause_schema_history WHERE script = ?", "V001__topic_pause.sql"))
            .as("each file applied once").isEqualTo(1);

        PostgresTaskStore tasks = new PostgresTaskStore(database.dataSource(), SchemaMode.VALIDATE);
        PostgresPauseStore store = new PostgresPauseStore(tasks);
        try (StoreTransaction transaction = tasks.begin()) {
            assertThat(store.list(transaction)).extracting(PauseStore.Paused::topic).containsExactly(UpgradeSeedRows.PAUSED);
            assertThat(store.pause(transaction, "new-topic")).isTrue();
            assertThat(store.resume(transaction, UpgradeSeedRows.PAUSED)).isTrue();
            assertThat(store.list(transaction)).extracting(PauseStore.Paused::topic).containsExactly("new-topic");
            transaction.commit();
        }
    }

    /** Before the first release: the rows the current code writes, dumped from a scratch schema. */
    private static String seedFromCurrentCode(PostgresDatabase database) throws Exception {
        String scratch = "pgtasks_seed";
        TaskSchema.migrate(database.dataSource(), scratch);
        UpgradeSeedRows.seed(new PostgresTaskStore(database.dataSource(), SchemaMode.MIGRATE, scratch));
        String seed = RELEASES.dump(database.dataSource(), scratch, "");
        database.execute("DROP SCHEMA " + scratch + " CASCADE");
        return seed.replace(ExtensionSchemaReleases.PLACEHOLDER, TaskSchema.DEFAULT_SCHEMA);
    }

    /** Pauses a release leaves behind: one paused topic. */
    static final class UpgradeSeedRows {
        static final String PAUSED = "held";

        static void seed(PostgresTaskStore tasks) throws Exception {
            PostgresPauseStore store = new PostgresPauseStore(tasks);
            try (StoreTransaction transaction = tasks.begin()) {
                assertThat(store.pause(transaction, PAUSED)).isTrue();
                transaction.commit();
            }
        }
    }
}
