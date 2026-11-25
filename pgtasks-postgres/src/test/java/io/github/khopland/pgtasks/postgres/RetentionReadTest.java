package io.github.khopland.pgtasks.postgres;

import java.sql.SQLException;
import java.time.Period;
import java.util.Map;

import io.github.khopland.pgtasks.Outcome;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/** Review 2026-11-25: one retention value that isn't an ISO period, edited in by hand, failed retention for every topic. */
class RetentionReadTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));

    @Test
    void a_retention_that_isnt_a_period_keeps_that_outcome_forever_and_nothing_else(PostgresDatabase database) throws SQLException {
        PostgresTaskStore store = new PostgresTaskStore(database.dataSource());
        int broken, fine;
        try (StoreTransaction transaction = store.begin()) {
            broken = store.createTopic(transaction, "broken");
            fine = store.createTopic(transaction, "fine");
            store.setRetention(transaction, fine, Map.of(Outcome.SUCCEEDED, Period.ofDays(30)));
            transaction.commit();
        }
        database.execute("UPDATE pgtasks.topic SET retention = '{\"succeeded\": \"a month\", \"filtered\": \"P7D\"}' WHERE id = " + broken);

        Map<Integer, Map<Outcome, Period>> retention;
        try (StoreTransaction transaction = store.begin()) {
            retention = store.retention(transaction);
        }

        assertThat(retention.get(broken)).containsExactly(Map.entry(Outcome.FILTERED, Period.ofDays(7)));
        assertThat(retention.get(fine)).containsExactly(Map.entry(Outcome.SUCCEEDED, Period.ofDays(30)));
    }
}
