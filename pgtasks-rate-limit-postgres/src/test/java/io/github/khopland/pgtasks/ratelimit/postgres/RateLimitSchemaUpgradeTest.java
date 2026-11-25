package io.github.khopland.pgtasks.ratelimit.postgres;

import java.util.List;
import java.util.stream.Stream;

import io.github.khopland.pgtasks.SchemaMode;
import io.github.khopland.pgtasks.postgres.ExtensionSchema;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.postgres.TaskSchema;
import io.github.khopland.pgtasks.ratelimit.SlidingCounter;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Upgrading the cluster rate limit's own table from every release keeps its counters. Each
 * release's files are applied with the extension's runner, its rows loaded, the current files
 * applied over them, and the counters read and updated by the current store. Until the first
 * release, the snapshot is taken of the current files in a scratch schema, as in
 * {@code SchemaUpgradeTest} for the engine's schema.
 */
class RateLimitSchemaUpgradeTest {

    static final String UNRELEASED = "unreleased";

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));

    static Stream<String> releases() {
        List<String> versions = RateLimitSchemaReleases.versions();
        return versions.isEmpty() ? Stream.of(UNRELEASED) : versions.stream();
    }

    @ParameterizedTest(name = "from {0}")
    @MethodSource("releases")
    void counters_survive_the_upgrade(String release, PostgresDatabase database) throws Exception {
        ExtensionSchema released;
        String seed;
        if (release.equals(UNRELEASED)) {
            released = PostgresSlidingWindowStore.SCHEMA;
            seed = seedFromCurrentCode(database);
        } else {
            released = RateLimitSchemaReleases.schema(release);
            seed = RateLimitSchemaReleases.seed(release);
        }
        released.migrate(database.dataSource(), TaskSchema.DEFAULT_SCHEMA);
        database.execute(seed.replace(RateLimitSchemaReleases.PLACEHOLDER, TaskSchema.DEFAULT_SCHEMA));

        PostgresSlidingWindowStore.SCHEMA.migrate(database.dataSource(), TaskSchema.DEFAULT_SCHEMA);
        assertThat(database.count("SELECT count(*) FROM pgtasks.rate_limit_schema_history WHERE script = ?", "V001__rate_limit_window.sql"))
            .as("each file applied once").isEqualTo(1);

        PostgresSlidingWindowStore store = new PostgresSlidingWindowStore(new PostgresTaskStore(database.dataSource(), SchemaMode.VALIDATE));
        assertThat(store.<Long>update(UpgradeSeedRows.BUSY, (state, now) -> state.current())).isEqualTo(UpgradeSeedRows.RESERVED);
        // The counter still counts: in the window it was left in, what remains of the limit is reserved.
        long admitted = store.update(UpgradeSeedRows.BUSY, (state, now) -> state.admit(now, UpgradeSeedRows.WINDOW, UpgradeSeedRows.LIMIT, 1_000).reserved());
        assertThat(admitted).isPositive().isLessThanOrEqualTo(UpgradeSeedRows.LIMIT);
        assertThat(store.<Long>update("new-topic", (state, now) -> state.admit(now, UpgradeSeedRows.WINDOW, 5, 5).reserved())).isEqualTo(5);
    }

    /** Before the first release: the rows the current code writes, dumped from a scratch schema. */
    private static String seedFromCurrentCode(PostgresDatabase database) throws Exception {
        String scratch = "pgtasks_seed";
        TaskSchema.migrate(database.dataSource(), scratch);
        UpgradeSeedRows.seed(new PostgresSlidingWindowStore(new PostgresTaskStore(database.dataSource(), SchemaMode.MIGRATE, scratch)));
        String seed = RateLimitSchemaReleases.dump(database, scratch, "");
        database.execute("DROP SCHEMA " + scratch + " CASCADE");
        return seed;
    }

    /** Counters a release leaves behind: one busy topic mid-window. */
    static final class UpgradeSeedRows {
        static final String BUSY = "busy";
        static final long WINDOW = 3_600_000;
        static final long LIMIT = 100;
        static final long RESERVED = 40;

        static void seed(PostgresSlidingWindowStore store) {
            SlidingCounter.Admission admitted = store.update(BUSY, (state, now) -> state.admit(now, WINDOW, LIMIT, (int) RESERVED));
            assertThat(admitted.reserved()).isEqualTo(RESERVED);
        }
    }
}
