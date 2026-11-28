package io.github.khopland.pgtasks.recurring.contract;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;

import io.github.khopland.pgtasks.recurring.ScheduleStore;
import io.github.khopland.pgtasks.recurring.ScheduleStore.Declaration;
import io.github.khopland.pgtasks.recurring.ScheduleStore.Key;
import io.github.khopland.pgtasks.recurring.ScheduleStore.Row;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What every {@link ScheduleStore} does, run against each store's: this module's in-memory one
 * and pgtasks-recurring-postgres's, which gets it from this module's test jar. In a package of its
 * own, apart from the module's, so that module's tests can use it. A subclass gives each test a fresh task store and its schedule
 * store. Times are relative to the store's clock, which on Postgres is the database's.
 */
public abstract class ScheduleStoreContract {

    protected abstract TaskStore taskStore();

    protected abstract ScheduleStore schedules();

    @FunctionalInterface
    protected interface Work<T> {
        T run(StoreTransaction transaction) throws SQLException;
    }

    protected final <T> T inTransaction(Work<T> work) throws SQLException {
        try (StoreTransaction transaction = taskStore().begin()) {
            T result = work.run(transaction);
            transaction.commit();
            return result;
        }
    }

    protected final Instant now() {
        // Microseconds, as Postgres keeps them.
        return taskStore().clock().instant().truncatedTo(ChronoUnit.MICROS);
    }

    private static final Key NIGHTLY = new Key("reports", "nightly");
    private static final Key HOURLY = new Key("reports", "hourly");
    private static final Key OTHER = new Key("billing", "nightly");

    private void declare(Declaration... declared) throws SQLException {
        inTransaction(transaction -> {
            schedules().declare(transaction, List.of(declared));
            return null;
        });
    }

    private List<Row> list() throws SQLException {
        return inTransaction(transaction -> schedules().list(transaction));
    }

    private Row row(Key key) throws SQLException {
        return list().stream().filter(row -> row.key().equals(key)).findFirst().orElseThrow();
    }

    private List<Key> due(Set<Key> keys, int limit) throws SQLException {
        return inTransaction(transaction -> schedules().due(transaction, keys, limit)).stream().map(Row::key).toList();
    }

    @Test
    void declaring_adds_a_schedule_and_keeps_its_next_run_while_its_cron_and_zone_stay() throws SQLException {
        Instant now = now();
        Instant first = now.plus(Duration.ofHours(2));
        declare(new Declaration(NIGHTLY, "0 2 * * *", "Europe/Oslo", first), new Declaration(OTHER, "0 3 * * *", "UTC", first));

        assertThat(list()).extracting(Row::key).as("by topic, then name").containsExactly(OTHER, NIGHTLY);
        Row stored = row(NIGHTLY);
        assertThat(stored.cron()).isEqualTo("0 2 * * *");
        assertThat(stored.zone()).isEqualTo("Europe/Oslo");
        assertThat(stored.nextRunAt()).isEqualTo(first);
        assertThat(stored.lastRunAt()).isNull();

        declare(new Declaration(NIGHTLY, "0 2 * * *", "Europe/Oslo", now.plus(Duration.ofHours(9))));
        assertThat(row(NIGHTLY).nextRunAt()).as("a restart with the same schedule").isEqualTo(first);
        assertThat(row(NIGHTLY).seenAt()).isAfterOrEqualTo(stored.seenAt());

        Instant moved = now.plus(Duration.ofHours(3));
        declare(new Declaration(NIGHTLY, "0 3 * * *", "Europe/Oslo", moved));
        assertThat(row(NIGHTLY).nextRunAt()).as("another cron").isEqualTo(moved);
        assertThat(row(NIGHTLY).cron()).isEqualTo("0 3 * * *");
        Instant zoned = now.plus(Duration.ofHours(4));
        declare(new Declaration(NIGHTLY, "0 3 * * *", "UTC", zoned));
        assertThat(row(NIGHTLY).nextRunAt()).as("another zone").isEqualTo(zoned);
    }

    @Test
    void due_schedules_are_the_ones_asked_for_whose_run_has_come_earliest_first() throws SQLException {
        Instant now = now();
        declare(new Declaration(NIGHTLY, "0 2 * * *", "UTC", now.minus(Duration.ofMinutes(1))),
            new Declaration(HOURLY, "0 * * * *", "UTC", now.minus(Duration.ofMinutes(5))),
            new Declaration(OTHER, "0 3 * * *", "UTC", now.minus(Duration.ofMinutes(9))),
            new Declaration(new Key("reports", "later"), "0 4 * * *", "UTC", now.plus(Duration.ofHours(1))));

        assertThat(due(Set.of(NIGHTLY, HOURLY, new Key("reports", "later")), 10)).containsExactly(HOURLY, NIGHTLY);
        assertThat(due(Set.of(NIGHTLY, HOURLY, OTHER), 2)).containsExactly(OTHER, HOURLY);
        assertThat(due(Set.of(new Key("reports", "unknown")), 10)).isEmpty();
        assertThat(due(Set.of(), 10)).isEmpty();
    }

    @Test
    void firing_moves_a_schedule_on_and_records_when_it_fired() throws SQLException {
        Instant now = now();
        declare(new Declaration(NIGHTLY, "0 2 * * *", "UTC", now.minus(Duration.ofMinutes(1))));
        Instant next = now.plus(Duration.ofDays(1));

        inTransaction(transaction -> {
            schedules().fired(transaction, NIGHTLY, now, next);
            return null;
        });
        assertThat(row(NIGHTLY).nextRunAt()).isEqualTo(next);
        assertThat(row(NIGHTLY).lastRunAt()).isEqualTo(now);
        assertThat(due(Set.of(NIGHTLY), 10)).isEmpty();

        Instant retry = now.plus(Duration.ofMinutes(1));
        inTransaction(transaction -> {
            schedules().fired(transaction, NIGHTLY, null, retry);
            return null;
        });
        assertThat(row(NIGHTLY).nextRunAt()).isEqualTo(retry);
        assertThat(row(NIGHTLY).lastRunAt()).as("put off, not fired").isEqualTo(now);
    }

    @Test
    void schedules_no_node_has_seen_since_a_time_are_forgotten() throws SQLException {
        Instant now = now();
        declare(new Declaration(NIGHTLY, "0 2 * * *", "UTC", now.plus(Duration.ofHours(1))),
            new Declaration(HOURLY, "0 * * * *", "UTC", now.plus(Duration.ofHours(1))));
        Instant seen = row(HOURLY).seenAt();

        inTransaction(transaction -> {
            schedules().seen(transaction, List.of(HOURLY, new Key("reports", "unknown")));
            return null;
        });
        assertThat(row(HOURLY).seenAt()).isAfterOrEqualTo(seen);
        assertThat(list()).as("seen adds nothing").hasSize(2);

        int kept = inTransaction(transaction -> schedules().forget(transaction, now.minus(Duration.ofHours(1))));
        assertThat(kept).isZero();
        int forgotten = inTransaction(transaction -> schedules().forget(transaction, now.plus(Duration.ofHours(1))));
        assertThat(forgotten).isEqualTo(2);
        assertThat(list()).isEmpty();
    }
}
