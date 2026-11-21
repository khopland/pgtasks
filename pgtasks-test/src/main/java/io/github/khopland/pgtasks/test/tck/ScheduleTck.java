package io.github.khopland.pgtasks.test.tck;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.admin.Schedule;
import io.github.khopland.pgtasks.spi.TaskStore.ScheduleSpec;

import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** TCK: recurring tasks as stored: synced per topic, found when due, moved on when fired. */
public abstract class ScheduleTck extends StoreTck {

    private static ScheduleSpec spec(String name, String cron, Instant next) {
        return new ScheduleSpec(name, "id-" + name, cron, "Europe/Oslo", Insertion.SUPERSEDE, "payload-" + name, "v1", next);
    }

    private static void sync(StoreFixture fixture, int topic, ScheduleSpec... specs) throws SQLException {
        fixture.inTransaction(transaction -> {
            fixture.store().syncSchedules(transaction, topic, List.of(specs));
            return null;
        });
    }

    /** Only this test's topics: a store's test may keep others in the background. */
    private static List<Schedule> schedules(StoreFixture fixture, String... topics) throws SQLException {
        List<String> wanted = List.of(topics);
        return fixture.inTransaction(transaction -> fixture.store().schedules(transaction)).stream()
            .filter(schedule -> wanted.contains(schedule.topic()))
            .toList();
    }

    private static List<Schedule> due(StoreFixture fixture) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().dueSchedules(transaction, 100));
    }

    @Test
    void a_topics_schedules_are_stored_as_given() throws SQLException {
        StoreFixture fixture = fixture();
        int topic = fixture.topic("reports");
        Instant next = fixture.store().clock().instant().plus(Duration.ofHours(1)).truncatedTo(ChronoUnit.SECONDS);
        sync(fixture, topic, spec("nightly", "0 2 * * *", next), spec("hourly", "@hourly", next));

        assertThat(schedules(fixture, "reports")).extracting(Schedule::name).containsExactly("hourly", "nightly");
        Schedule nightly = schedules(fixture, "reports").get(1);
        assertThat(nightly.topicId()).isEqualTo(topic);
        assertThat(nightly.identifier()).isEqualTo("id-nightly");
        assertThat(nightly.cron()).isEqualTo("0 2 * * *");
        assertThat(nightly.zone()).isEqualTo("Europe/Oslo");
        assertThat(nightly.insertion()).isEqualTo(Insertion.SUPERSEDE);
        assertThat(nightly.payload()).isEqualTo("payload-nightly");
        assertThat(nightly.payloadType()).isEqualTo("v1");
        assertThat(nightly.nextRunAt()).isEqualTo(next);
        assertThat(nightly.lastRunAt()).isNull();
    }

    @Test
    void syncing_again_keeps_the_next_run_of_an_unchanged_cron_and_removes_what_is_gone() throws SQLException {
        StoreFixture fixture = fixture();
        int topic = fixture.topic("reports"), other = fixture.topic("other");
        Instant now = fixture.store().clock().instant().truncatedTo(ChronoUnit.SECONDS);
        Instant first = now.plus(Duration.ofHours(1)), later = now.plus(Duration.ofHours(5));
        sync(fixture, topic, spec("kept", "0 2 * * *", first), spec("changed", "0 3 * * *", first), spec("gone", "0 4 * * *", first));
        sync(fixture, other, spec("elsewhere", "0 4 * * *", first));

        sync(fixture, topic, spec("kept", "0 2 * * *", later), spec("changed", "0 5 * * *", later), spec("new", "0 6 * * *", later));

        assertThat(schedules(fixture, "reports")).extracting(Schedule::name, Schedule::cron, Schedule::nextRunAt).containsExactly(
            Tuple.tuple("changed", "0 5 * * *", later),
            Tuple.tuple("kept", "0 2 * * *", first),
            Tuple.tuple("new", "0 6 * * *", later));
        assertThat(schedules(fixture, "other")).extracting(Schedule::name).containsExactly("elsewhere");

        sync(fixture, topic);
        assertThat(schedules(fixture, "reports")).isEmpty();
    }

    @Test
    void due_schedules_are_found_earliest_first_and_move_on_when_fired() throws SQLException {
        StoreFixture fixture = fixture();
        int topic = fixture.topic("reports");
        Instant now = fixture.store().clock().instant().truncatedTo(ChronoUnit.SECONDS);
        sync(fixture, topic, spec("late", "0 2 * * *", now.minus(Duration.ofMinutes(1))),
            spec("later", "0 3 * * *", now.minus(Duration.ofMinutes(10))),
            spec("future", "0 4 * * *", now.plus(Duration.ofHours(1))));

        List<Schedule> due = due(fixture).stream().filter(schedule -> schedule.topicId() == topic).toList();
        assertThat(due).extracting(Schedule::name).containsExactly("later", "late");

        Instant next = now.plus(Duration.ofDays(1));
        fixture.inTransaction(transaction -> {
            fixture.store().scheduleFired(transaction, due.getFirst().id(), now, next);
            return null;
        });
        Schedule fired = schedules(fixture, "reports").stream().filter(schedule -> schedule.name().equals("later")).findFirst().orElseThrow();
        assertThat(fired.lastRunAt()).isEqualTo(now);
        assertThat(fired.nextRunAt()).isEqualTo(next);
        assertThat(due(fixture).stream().filter(schedule -> schedule.topicId() == topic)).extracting(Schedule::name).containsExactly("late");
    }
}
