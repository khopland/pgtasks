package io.github.khopland.pgtasks.recurring.postgres;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.postgres.TaskSchema;
import io.github.khopland.pgtasks.recurring.ScheduleStore;
import io.github.khopland.pgtasks.recurring.ScheduleStore.Declaration;
import io.github.khopland.pgtasks.recurring.ScheduleStore.Key;
import io.github.khopland.pgtasks.recurring.ScheduleStore.Row;
import io.github.khopland.pgtasks.recurring.contract.ScheduleStoreContract;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresScheduleStoreTest extends ScheduleStoreContract {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));

    private PostgresTaskStore store;
    private PostgresScheduleStore schedules;

    @BeforeEach
    void setUp(PostgresDatabase database) {
        store = new PostgresTaskStore(database.dataSource());
        schedules = new PostgresScheduleStore(store);
        schedules.prepare();
    }

    @Override
    protected TaskStore taskStore() {
        return store;
    }

    @Override
    protected ScheduleStore schedules() {
        return schedules;
    }

    @Test
    void a_due_run_another_transaction_holds_is_skipped(PostgresDatabase database) throws SQLException {
        Key first = new Key("reports", "first");
        Key second = new Key("reports", "second");
        Instant past = now().minus(Duration.ofMinutes(1));
        inTransaction(transaction -> {
            schedules.declare(transaction, List.of(new Declaration(first, "0 * * * *", "UTC", past),
                new Declaration(second, "0 * * * *", "UTC", past.plusSeconds(1))));
            return null;
        });

        try (StoreTransaction holding = store.begin()) {
            assertThat(schedules.due(holding, Set.of(first), 10)).extracting(Row::key).containsExactly(first);
            // Another node, meanwhile: it neither waits nor fires the held run.
            List<Key> other = inTransaction(transaction -> schedules.due(transaction, Set.of(first, second), 10)).stream().map(Row::key).toList();
            assertThat(other).containsExactly(second);
            schedules.fired(holding, first, now(), now().plus(Duration.ofHours(1)));
            holding.commit();
        }
        List<Row> after = inTransaction(transaction -> schedules.due(transaction, Set.of(first), 10));
        assertThat(after).as("moved on by the holder").isEmpty();
        assertThat(database.count("SELECT count(*) FROM pgtasks.recurring_schema_history WHERE script = ?", "V001__recurring_schedule.sql"))
            .isEqualTo(1);
    }
}
