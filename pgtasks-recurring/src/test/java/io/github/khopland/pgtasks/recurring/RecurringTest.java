package io.github.khopland.pgtasks.recurring;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.PayloadCodec;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.EngineHealth;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskEngineBuilder;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.engine.extension.OperatorView;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.ManualClock;
import io.github.khopland.pgtasks.spi.StoreTransaction;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Recurring tasks on the in-memory store, with a clock that moves only when the test says. */
class RecurringTest {

    private static final TopicRef<String> REPORTS = TopicRef.of("reports", String.class);
    private static final TopicRef<String> WAITING = TopicRef.of("waiting", String.class);
    private static final TopicRef<Flaky> FLAKY = TopicRef.of("flaky", Flaky.class);
    private static final Cron HOURLY = Cron.parse("0 * * * *");

    /** Thirty seconds past midnight, so the first hourly run is at 01:00. */
    private final ManualClock clock = new ManualClock(Instant.parse("2026-11-28T00:00:30Z"));
    private final InMemoryTaskStore store = new InMemoryTaskStore(clock);
    private final List<String> ran = new CopyOnWriteArrayList<>();
    private final List<TaskEngine> engines = new ArrayList<>();
    /** Over every engine started so far; it records the pushes committed since it was made. */
    private @Nullable TaskTester tester;
    /** While set, encoding a {@link Flaky} payload fails, as a push that can't be written would. */
    private final AtomicBoolean failing = new AtomicBoolean();

    record Flaky(String name) {
    }

    @AfterEach
    void stop() {
        engines.forEach(TaskEngine::close);
    }

    private TaskEngineBuilder builder(Recurring recurring) {
        return TaskEngine.builder()
            .store(store)
            .codec(Flaky.class, PayloadCodec.of(payload -> {
                if (failing.get()) {
                    throw new IllegalStateException("can't encode " + payload);
                }
                return payload.name();
            }, Flaky::new))
            .configure("reports", tasks -> tasks
                .eachTask(REPORTS, task -> ran.add(task.payload()))
                .eachTask(FLAKY, task -> ran.add(task.payload().name()))
                .pushesTo(WAITING))
            .extension(recurring);
    }

    private TaskEngine start(Recurring recurring) {
        TaskEngine engine = builder(recurring).build().start();
        engines.add(engine);
        tester = TaskTester.of(engines);
        return engine;
    }

    private void close(TaskEngine engine) {
        engine.close();
        engines.remove(engine);
        tester = engines.isEmpty() ? null : TaskTester.of(engines);
    }

    private TaskTester tester() {
        return Objects.requireNonNull(tester, "no engine runs");
    }

    /** Moves the clock to this time today, runs the jobs, and waits for the tasks pushed. */
    private void at(String time) {
        Instant target = Instant.parse("2026-11-28T" + time + "Z");
        tester().advance(Duration.between(clock.instant(), target));
        tester().awaitIdle();
    }

    private List<ScheduleStore.Row> stored() throws SQLException {
        ScheduleStore schedules = new InMemoryScheduleCapability().create(ScheduleStore.class, store).orElseThrow();
        try (StoreTransaction transaction = store.begin()) {
            return schedules.list(transaction);
        }
    }

    @Test
    @DisplayName("A run fires once when due, and runs missed meanwhile are made up once")
    void fires_when_due_and_makes_up_missed_runs_once() throws SQLException {
        start(Recurring.of(Schedule.of(REPORTS, "hourly", HOURLY, "report")));
        assertThat(stored()).singleElement().satisfies(row -> {
            assertThat(row.nextRunAt()).isEqualTo(Instant.parse("2026-11-28T01:00:00Z"));
            assertThat(row.cron()).isEqualTo("0 * * * *");
            assertThat(row.zone()).isEqualTo("Z");
        });

        at("00:59:59");
        assertThat(ran).isEmpty();
        at("01:00:00");
        assertThat(ran).containsExactly("report");
        at("01:30:00");
        assertThat(ran).as("once per run").containsExactly("report");

        at("05:20:00");
        assertThat(ran).as("02:00 to 05:00 were missed, and are made up once").containsExactly("report", "report");
        assertThat(stored().getFirst().nextRunAt()).isEqualTo(Instant.parse("2026-11-28T06:00:00Z"));
        assertThat(stored().getFirst().lastRunAt()).isEqualTo(Instant.parse("2026-11-28T05:20:00Z"));
        assertThat(tester().pushedIdentifiers(REPORTS)).containsOnly("hourly");
    }

    @Test
    @DisplayName("Engines that declare the same schedule on one store fire each run once")
    void two_nodes_fire_once() {
        Recurring recurring = Recurring.of(Schedule.of(REPORTS, "hourly", HOURLY, "report"));
        start(recurring);
        start(recurring);

        at("01:00:01");
        at("02:00:01");
        assertThat(ran).containsExactly("report", "report");
        tester().assertPushed(REPORTS, 2);
    }

    @Test
    @DisplayName("A restart keeps the next run; a new cron moves it")
    void a_restart_keeps_the_next_run_unless_the_cron_changed() throws SQLException {
        TaskEngine first = start(Recurring.of(Schedule.of(REPORTS, "report", Cron.parse("0 3 * * *"), "report")));
        assertThat(stored().getFirst().nextRunAt()).isEqualTo(Instant.parse("2026-11-28T03:00:00Z"));
        close(first);

        clock.advance(Duration.ofMinutes(30));
        TaskEngine second = start(Recurring.of(Schedule.of(REPORTS, "report", Cron.parse("0 3 * * *"), "report")));
        assertThat(stored().getFirst().nextRunAt()).as("the same cron").isEqualTo(Instant.parse("2026-11-28T03:00:00Z"));
        close(second);

        start(Recurring.of(Schedule.of(REPORTS, "report", Cron.parse("*/15 * * * *"), "report")));
        assertThat(stored().getFirst().nextRunAt()).as("a new cron").isEqualTo(Instant.parse("2026-11-28T00:45:00Z"));
        assertThat(stored().getFirst().cron()).isEqualTo("*/15 * * * *");
    }

    @Test
    @DisplayName("A run whose push fails is tried again a minute later, and the others still fire")
    void a_failing_push_is_retried_and_holds_back_nothing() throws SQLException {
        start(Recurring.of(
            Schedule.of(FLAKY, "flaky", HOURLY, new Flaky("flaky")),
            Schedule.of(REPORTS, "hourly", HOURLY, "report")));
        failing.set(true);

        at("01:00:10");
        assertThat(ran).containsExactly("report");
        assertThat(stored()).filteredOn(row -> row.key().name().equals("flaky")).singleElement().satisfies(row -> {
            assertThat(row.nextRunAt()).isEqualTo(Instant.parse("2026-11-28T01:01:10Z"));
            assertThat(row.lastRunAt()).as("put off, not fired").isNull();
        });

        failing.set(false);
        at("01:01:00");
        assertThat(ran).as("not before the retry").containsExactly("report");
        at("01:01:10");
        assertThat(ran).containsExactly("report", "flaky");
        at("02:00:00");
        assertThat(ran).as("and on its schedule after that").hasSize(4).containsOnly("report", "flaky");
        assertThat(ran.stream().filter("flaky"::equals)).hasSize(2);
    }

    @Test
    @DisplayName("Runs that wait supersede each other unless the schedule appends")
    void waiting_runs_supersede_each_other() {
        start(Recurring.of(
            Schedule.of(WAITING, "superseding", HOURLY, "superseding"),
            Schedule.of(WAITING, "appending", HOURLY, "appending").withInsertion(Insertion.APPEND)));

        at("01:00:00");
        at("02:00:00");
        at("03:00:00");
        assertThat(tester().counts(WAITING).total()).as("one superseding run waits, and three appending ones").isEqualTo(4);
        assertThat(tester().pushedIdentifiers(WAITING)).containsOnly("superseding", "appending").hasSize(6);
    }

    @Test
    @DisplayName("A schedule no node declares stops firing, and is deleted after 30 days")
    void a_schedule_no_node_declares_is_forgotten() throws SQLException {
        TaskEngine old = start(Recurring.of(Schedule.of(REPORTS, "renamed", HOURLY, "old")));
        close(old);
        start(Recurring.of(Schedule.of(REPORTS, "hourly", HOURLY, "new")));

        at("01:00:00");
        assertThat(ran).as("only what a running node declares").containsExactly("new");
        assertThat(stored()).extracting(row -> row.key().name()).containsExactly("hourly", "renamed");

        tester().advance(Duration.ofDays(31));
        assertThat(stored()).extracting(row -> row.key().name()).containsExactly("hourly");
    }

    @Test
    @DisplayName("Operators see every stored schedule, and can run one of this node's now")
    void status_and_run_now() throws Exception {
        TaskEngine other = start(Recurring.of(Schedule.of(REPORTS, "elsewhere", Cron.parse("0 4 * * *"), "elsewhere")));
        Recurring recurring = Recurring.of(Schedule.of(REPORTS, "hourly", HOURLY, "report"));
        assertThatThrownBy(recurring::status).isInstanceOf(IllegalStateException.class).hasMessageContaining("installed");
        start(recurring);
        close(other);

        assertThat(recurring.status()).extracting(Recurring.Status::name, Recurring.Status::declared, Recurring.Status::nextRunAt)
            .containsExactly(
                org.assertj.core.groups.Tuple.tuple("elsewhere", false, Instant.parse("2026-11-28T04:00:00Z")),
                org.assertj.core.groups.Tuple.tuple("hourly", true, Instant.parse("2026-11-28T01:00:00Z")));

        assertThat(recurring.runNow("reports", "elsewhere")).as("another node's").isFalse();
        assertThat(recurring.runNow("reports", "missing")).isFalse();
        assertThat(recurring.runNow("reports", "hourly")).isTrue();
        await("the run asked for, without waiting for the clock", () -> ran.contains("report"));
        assertThat(recurring.status()).filteredOn(Recurring.Status::declared).singleElement().satisfies(status -> {
            assertThat(status.lastRunAt()).isEqualTo(Instant.parse("2026-11-28T00:00:30Z"));
            assertThat(status.nextRunAt()).as("the next run is on the schedule again").isEqualTo(Instant.parse("2026-11-28T01:00:00Z"));
        });
        at("00:59:00");
        assertThat(ran).containsExactly("report");
    }

    @Test
    @DisplayName("The schedules view shows every stored schedule, with Run now on this node's")
    void the_schedules_view() throws Exception {
        TaskEngine other = start(Recurring.of(Schedule.of(REPORTS, "elsewhere", Cron.parse("0 4 * * *"), "elsewhere")));
        TaskEngine engine = start(Recurring.of(Schedule.of(REPORTS, "hourly", HOURLY, "report")));
        close(other);
        OperatorView view = engine.operatorViews().stream().filter(each -> each.name().equals("schedules")).findFirst().orElseThrow();

        assertThat(view.rows()).extracting(OperatorView.Row::key).containsExactly("reports/elsewhere", "reports/hourly");
        assertThat(view.rows()).filteredOn(row -> row.key().equals("reports/elsewhere")).singleElement()
            .satisfies(row -> {
                assertThat(row.actions()).as("only the node that declares it runs it").isEmpty();
                assertThat(row.values()).containsEntry("declared", false).doesNotContainKey("lastRunAt");
            });
        OperatorView.Action run = view.actions().getFirst();
        run.perform().perform("reports/hourly");
        await("the run", () -> ran.contains("report"));
        assertThatThrownBy(() -> run.perform().perform("reports/elsewhere")).hasMessageContaining("that this node declares");
    }

    private static void await(String what, java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting for " + what);
            }
            Thread.sleep(5);
        }
    }

    @Test
    @DisplayName("Mistakes in the schedules fail when they are declared")
    void mistakes_fail_early() {
        assertThatThrownBy(() -> Recurring.of(Schedule.of(REPORTS, "same", HOURLY, "a"), Schedule.of(REPORTS, "same", HOURLY, "b")))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("reports").hasMessageContaining("same");
        assertThat(Recurring.of(Schedule.of(REPORTS, "same", HOURLY, "a"), Schedule.of(WAITING, "same", HOURLY, "b")).schedules())
            .as("a name is unique within its topic").hasSize(2);
        assertThatThrownBy(() -> Schedule.of(REPORTS, " ", HOURLY, "a")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Schedule.of(REPORTS, "x".repeat(201), HOURLY, "a")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Schedule.of(REPORTS, "long", HOURLY, "a").withIdentifier("x".repeat(3_000)))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TaskEngine.builder().set(Recurring.INTERVAL, Duration.ZERO))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("recurring.interval: must be positive");
        assertThat(Recurring.INTERVAL.parse("5s")).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("With no schedules, the extension does nothing")
    void no_schedules_no_job() throws SQLException {
        start(Recurring.of());
        at("05:00:00");
        assertThat(stored()).isEmpty();
        assertThat(engines.getFirst().extensions()).containsExactly("Recurring");
        assertThat(engines.getFirst().health().jobs()).extracting(EngineHealth.JobStatus::name).doesNotContain(Recurring.JOB);
    }
}
