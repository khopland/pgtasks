package io.github.khopland.pgtasks.recurring.postgres;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import io.github.khopland.pgtasks.SchemaMode;
import io.github.khopland.pgtasks.postgres.ExtensionSchema;
import io.github.khopland.pgtasks.postgres.ExtensionSchemaReleases;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.postgres.TaskSchema;
import io.github.khopland.pgtasks.recurring.ScheduleStore.Declaration;
import io.github.khopland.pgtasks.recurring.ScheduleStore.Key;
import io.github.khopland.pgtasks.recurring.ScheduleStore.Row;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Upgrading recurring tasks' own table from every release keeps its schedules. Each release's
 * files are applied with the extension's runner, its rows loaded, the current files applied over
 * them, and the schedules read and fired by the current store. Until the first release, the
 * snapshot is taken of the current files in a scratch schema, as in {@code SchemaUpgradeTest}.
 */
class RecurringSchemaUpgradeTest {

    static final String UNRELEASED = "unreleased";
    static final ExtensionSchemaReleases RELEASES =
        ExtensionSchemaReleases.of(PostgresScheduleStore.SCHEMA, "io/github/khopland/pgtasks/recurring/postgres/released");

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));

    static Stream<String> releases() {
        List<String> versions = RELEASES.versions();
        return versions.isEmpty() ? Stream.of(UNRELEASED) : versions.stream();
    }

    @ParameterizedTest(name = "from {0}")
    @MethodSource("releases")
    void schedules_survive_the_upgrade(String release, PostgresDatabase database) throws Exception {
        ExtensionSchema released;
        String seed;
        if (release.equals(UNRELEASED)) {
            released = PostgresScheduleStore.SCHEMA;
            seed = seedFromCurrentCode(database);
        } else {
            released = RELEASES.schema(release);
            seed = RELEASES.seed(release, TaskSchema.DEFAULT_SCHEMA);
        }
        released.migrate(database.dataSource(), TaskSchema.DEFAULT_SCHEMA);
        database.execute(seed);

        PostgresScheduleStore.SCHEMA.migrate(database.dataSource(), TaskSchema.DEFAULT_SCHEMA);
        assertThat(database.count("SELECT count(*) FROM pgtasks.recurring_schema_history WHERE script = ?", "V001__recurring_schedule.sql"))
            .as("each file applied once").isEqualTo(1);

        PostgresTaskStore store = new PostgresTaskStore(database.dataSource(), SchemaMode.VALIDATE);
        PostgresScheduleStore schedules = new PostgresScheduleStore(store);
        schedules.prepare();
        try (StoreTransaction transaction = store.begin()) {
            List<Row> rows = schedules.list(transaction);
            assertThat(rows).extracting(Row::key).containsExactly(UpgradeSeedRows.DUE, UpgradeSeedRows.FIRED);
            assertThat(rows.get(1).lastRunAt()).isNotNull();
            assertThat(schedules.due(transaction, Set.of(UpgradeSeedRows.DUE, UpgradeSeedRows.FIRED), 10)).extracting(Row::key)
                .containsExactly(UpgradeSeedRows.DUE);
            schedules.fired(transaction, UpgradeSeedRows.DUE, Instant.now(), Instant.now().plus(Duration.ofDays(1)));
            transaction.commit();
        }
    }

    /** Before the first release: the rows the current code writes, dumped from a scratch schema. */
    private static String seedFromCurrentCode(PostgresDatabase database) throws Exception {
        String scratch = "pgtasks_seed";
        TaskSchema.migrate(database.dataSource(), scratch);
        UpgradeSeedRows.seed(new PostgresTaskStore(database.dataSource(), SchemaMode.MIGRATE, scratch));
        String seed = RELEASES.dump(database.dataSource(), scratch, "").replace(ExtensionSchemaReleases.PLACEHOLDER, TaskSchema.DEFAULT_SCHEMA);
        database.execute("DROP SCHEMA " + scratch + " CASCADE");
        return seed;
    }

    /** Schedules a release leaves behind: one whose run is due, and one that fired and waits for its next. */
    static final class UpgradeSeedRows {
        static final Key DUE = new Key("reports", "due");
        static final Key FIRED = new Key("reports", "fired");

        static void seed(PostgresTaskStore store) throws SQLException {
            PostgresScheduleStore schedules = new PostgresScheduleStore(store);
            schedules.prepare();
            Instant now = Instant.now();
            try (StoreTransaction transaction = store.begin()) {
                schedules.declare(transaction, List.of(
                    new Declaration(DUE, "0 2 * * *", "Europe/Oslo", now.minus(Duration.ofHours(1))),
                    new Declaration(FIRED, "@hourly", "UTC", now.minus(Duration.ofMinutes(5)))));
                schedules.fired(transaction, FIRED, now.minus(Duration.ofMinutes(5)), now.plus(Duration.ofDays(3650)));
                transaction.commit();
            }
        }
    }
}
