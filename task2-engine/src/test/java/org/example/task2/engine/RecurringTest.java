package org.example.task2.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.example.task2.admin.Schedule;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.example.task2.Decision;
import org.example.task2.TopicRef;
import org.example.task2.memory.InMemoryTaskStore;
import org.example.task2.spi.ManualClock;
import org.example.task2.spi.TaskStore;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Recurring tasks: pushed by the housekeeping leader when due, once across the cluster. */
class RecurringTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final TopicRef<String> REPORTS = TopicRef.of("reports", String.class);
    private static final Instant START = Instant.parse("2026-11-21T01:00:00Z");

    private final ManualClock clock = new ManualClock(START);
    private final InMemoryTaskStore store = new InMemoryTaskStore(clock);
    private final List<String> ran = new CopyOnWriteArrayList<>();
    private final List<TaskEngine> engines = new CopyOnWriteArrayList<>();

    @AfterEach
    void stop() {
        engines.forEach(TaskEngine::close);
    }

    private TaskEngine start(String node, String cron) throws Exception {
        TaskEngine engine = TaskEngine.builder()
            .store(store)
            .node(node)
            .configure("reporting", tasks -> tasks
                .topic(REPORTS, batch -> batch.decide(task -> {
                    ran.add(task.identifier() + ":" + task.payload());
                    return Decision.success();
                }))
                .recurring(REPORTS, "nightly", Cron.parse(cron), "nightly-report", "full"))
            .build()
            .start();
        engines.add(engine);
        return engine;
    }

    @Test
    @DisplayName("Fires when due, once; a long gap is made up once")
    void fires_when_due_and_makes_up_a_gap_once() throws Exception {
        TaskEngine engine = start("node-a", "0 2 * * *");
        TaskTester tester = TaskTester.of(engine);
        assertThat(engine.admin().schedules()).singleElement().satisfies(schedule -> {
            assertThat(schedule.nextRunAt()).isEqualTo(Instant.parse("2026-11-21T02:00:00Z"));
            assertThat(schedule.lastRunAt()).isNull();
        });

        tester.advance(Duration.ofMinutes(59));
        tester.awaitIdle();
        assertThat(ran).isEmpty();

        tester.advance(Duration.ofMinutes(1));
        tester.awaitIdle();
        assertThat(ran).containsExactly("nightly-report:full");
        tester.advance(Duration.ofHours(1));
        tester.awaitIdle();
        assertThat(ran).hasSize(1);

        // Three days without a leader: one run, then on schedule from now.
        tester.advance(Duration.ofDays(3));
        tester.awaitIdle();
        assertThat(ran).hasSize(2);
        assertThat(engine.admin().schedules().getFirst().nextRunAt()).isEqualTo(Instant.parse("2026-11-25T02:00:00Z"));
    }

    @Test
    @DisplayName("While the topic is paused, runs don't pile up: each supersedes the waiting one")
    void runs_supersede_each_other_while_waiting() throws Exception {
        TaskEngine engine = start("node-a", "0 * * * *");
        TaskTester tester = TaskTester.of(engine);
        engine.admin().pause("reports");
        for (int hour = 0; hour < 3; hour++) {
            tester.advance(Duration.ofHours(1));
        }
        assertThat(tester.counts(REPORTS).ready()).isEqualTo(1);

        engine.admin().resume("reports");
        tester.awaitIdle();
        assertThat(ran).hasSize(1);
    }

    @Test
    @DisplayName("Two nodes with the same schedule push it once")
    void two_nodes_fire_once() throws Exception {
        TaskTester tester = TaskTester.of(start("node-a", "0 2 * * *"), start("node-b", "0 2 * * *"));

        tester.advance(Duration.ofHours(1));
        tester.awaitIdle();

        assertThat(ran).containsExactly("nightly-report:full");
    }

    @Test
    @DisplayName("A node starting with another cron replaces the schedule; one without it removes it")
    void the_configuration_at_start_replaces_the_stored_schedule() throws Exception {
        start("node-a", "0 2 * * *").close();
        TaskEngine changed = start("node-a", "30 3 * * *");
        assertThat(changed.admin().schedules()).extracting(Schedule::cron, Schedule::nextRunAt)
            .containsExactly(org.assertj.core.groups.Tuple.tuple("30 3 * * *", Instant.parse("2026-11-21T03:30:00Z")));
        changed.close();

        TaskEngine without = TaskEngine.builder().store(store)
            .configure("reporting", tasks -> tasks.topic(REPORTS, batch -> batch.decide(task -> Decision.success())))
            .build().start();
        engines.add(without);
        assertThat(without.admin().schedules()).isEmpty();
    }

    @Test
    @DisplayName("On Postgres: the leader pushes a due schedule and moves it on in one transaction")
    void fires_on_postgres(PostgresDatabase database) throws Exception {
        TaskEngine engine = TaskEngine.builder()
            .dataSource(database.dataSource())
            .globals(globals -> globals.housekeeping(housekeeping -> housekeeping.promoteEvery(Duration.ofMillis(50))))
            .configure("reporting", tasks -> tasks
                .topic(REPORTS, batch -> batch.decide(task -> {
                    ran.add(task.identifier() + ":" + task.payload());
                    return Decision.success();
                }))
                .recurring(REPORTS, "nightly", Cron.parse("0 2 * * *", java.time.ZoneId.of("Europe/Oslo")), "nightly-report", "full"))
            .build()
            .start();
        engines.add(engine);
        assertThat(database.count("SELECT count(*) FROM tasks.schedule WHERE zone = 'Europe/Oslo' AND next_run_at > now()")).isEqualTo(1);

        database.execute("UPDATE tasks.schedule SET next_run_at = now() - interval '1 second'");
        EngineFixture.await("the recurring task ran", Duration.ofSeconds(5), () -> !ran.isEmpty());

        assertThat(ran).containsExactly("nightly-report:full");
        assertThat(database.count("SELECT count(*) FROM tasks.schedule WHERE last_run_at IS NOT NULL AND next_run_at > now()")).isEqualTo(1);
    }

    @Test
    @DisplayName("A module schedules only its own topics, and names are unique per topic")
    void mistakes_fail_the_build() {
        assertThatThrownBy(() -> TaskEngine.builder().store(store)
            .configure("reporting", tasks -> tasks.recurring(REPORTS, "nightly", Cron.parse("@daily"), "x", "y"))
            .build()).hasMessageContaining("doesn't register before it");
        assertThatThrownBy(() -> TaskEngine.builder().store(store)
            .configure("reporting", tasks -> tasks.topic(REPORTS, batch -> batch.decide(task -> Decision.success()))
                .recurring(REPORTS, "nightly", Cron.parse("@daily"), "x", "y")
                .recurring(REPORTS, "nightly", Cron.parse("@hourly"), "x", "y"))
            .build()).hasMessageContaining("Two recurring tasks named nightly");
    }
}
