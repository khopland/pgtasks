package io.github.khopland.pgtasks.recurring.postgres;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import io.github.khopland.pgtasks.PayloadCodec;
import io.github.khopland.pgtasks.SchemaMode;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.postgres.TaskSchema;
import io.github.khopland.pgtasks.recurring.Cron;
import io.github.khopland.pgtasks.recurring.Recurring;
import io.github.khopland.pgtasks.recurring.Schedule;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Recurring tasks on Postgres: every node fires what it declares, and each run once. */
class RecurringPostgresTest {

    private static final TopicRef<String> REPORTS = TopicRef.of("reports", String.class);
    private static final TopicRef<Broken> BROKEN = TopicRef.of("broken", Broken.class);
    /** Never due by itself in a test: each test makes a run due by moving next_run_at back. */
    private static final Cron YEARLY = Cron.parse("@yearly");

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));

    private final List<TaskEngine> engines = new ArrayList<>();
    private final List<String> ran = new CopyOnWriteArrayList<>();

    record Broken(String name) {
    }

    @AfterEach
    void stop() {
        engines.forEach(TaskEngine::close);
    }

    private TaskEngine start(PostgresDatabase database, String node, Recurring recurring) {
        TaskEngine engine = TaskEngine.builder()
            .dataSource(database.dataSource())
            .node(node)
            .codec(Broken.class, PayloadCodec.of(payload -> {
                throw new IllegalStateException("can't encode " + payload);
            }, Broken::new))
            .configure("reports", tasks -> tasks
                .eachTask(REPORTS, task -> ran.add(task.payload()))
                .eachTask(BROKEN, task -> ran.add(task.payload().name())))
            .extension(recurring)
            .set(Recurring.INTERVAL, Duration.ofMillis(50))
            .build()
            .start();
        engines.add(engine);
        return engine;
    }

    private static void due(PostgresDatabase database, String name) {
        database.execute("UPDATE pgtasks.recurring_schedule SET next_run_at = clock_timestamp() - interval '1 second' WHERE name = '" + name + "'");
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting for " + what);
            }
            Thread.sleep(20);
        }
    }

    @Test
    void nodes_that_declare_a_schedule_push_each_run_once(PostgresDatabase database) throws Exception {
        Recurring recurring = Recurring.of(Schedule.of(REPORTS, "yearly", YEARLY, "report"));
        start(database, "node-a", recurring);
        start(database, "node-b", recurring);
        start(database, "node-c", recurring);
        assertThat(database.count("SELECT count(*) FROM pgtasks.recurring_schedule WHERE topic = 'reports' AND name = 'yearly' AND cron = '@yearly'"))
            .isEqualTo(1);

        for (int run = 1; run <= 5; run++) {
            due(database, "yearly");
            int expected = run;
            await("run " + run, () -> ran.size() >= expected);
            await("the schedule to move on", () ->
                database.count("SELECT count(*) FROM pgtasks.recurring_schedule WHERE next_run_at > clock_timestamp()") == 1);
        }
        Thread.sleep(300);
        assertThat(ran).as("five runs, each pushed by one of three nodes").hasSize(5);
        assertThat(database.count("SELECT count(*) FROM pgtasks.task WHERE identifier = 'yearly'")).isEqualTo(5);
        assertThat(database.count("SELECT count(*) FROM pgtasks.recurring_schedule WHERE last_run_at IS NOT NULL")).isEqualTo(1);
    }

    @Test
    void a_run_whose_push_fails_is_rolled_back_alone(PostgresDatabase database) throws Exception {
        start(database, "node-a", Recurring.of(Schedule.of(BROKEN, "broken", YEARLY, new Broken("broken")),
            Schedule.of(REPORTS, "yearly", YEARLY, "report")));
        due(database, "broken");
        due(database, "yearly");

        await("the other schedule's run", () -> ran.contains("report"));
        await("the failed run to be put off", () -> database.count(
            "SELECT count(*) FROM pgtasks.recurring_schedule WHERE name = 'broken' AND last_run_at IS NULL "
                + "AND next_run_at BETWEEN clock_timestamp() + interval '30 seconds' AND clock_timestamp() + interval '61 seconds'") == 1);
        assertThat(database.count("SELECT count(*) FROM pgtasks.task WHERE identifier = 'broken'")).as("nothing of it was written").isZero();
        assertThat(database.count("SELECT count(*) FROM pgtasks.recurring_schedule WHERE name = 'yearly' AND last_run_at IS NOT NULL"))
            .isEqualTo(1);
    }

    @Test
    void validate_mode_without_the_table_fails_the_start(PostgresDatabase database) throws Exception {
        assertThatThrownBy(() -> TaskEngine.builder()
            .dataSource(database.dataSource())
            .schemaMode(SchemaMode.VALIDATE)
            .configure("reports", tasks -> tasks.eachTask(REPORTS, task -> ran.add(task.payload())))
            .extension(Recurring.of(Schedule.of(REPORTS, "yearly", YEARLY, "report")))
            .build()
            .start())
            .hasMessageContaining("recurring_schedule")
            .hasMessageContaining("V001__recurring_schedule.sql");

        PostgresScheduleStore.SCHEMA.migrate(database.dataSource(), TaskSchema.DEFAULT_SCHEMA);
        TaskEngine engine = TaskEngine.builder()
            .dataSource(database.dataSource())
            .schemaMode(SchemaMode.VALIDATE)
            .configure("reports", tasks -> tasks.eachTask(REPORTS, task -> ran.add(task.payload())))
            .extension(Recurring.of(Schedule.of(REPORTS, "yearly", YEARLY, "report")))
            .build()
            .start();
        engines.add(engine);
        assertThat(database.count("SELECT count(*) FROM pgtasks.recurring_schedule")).as("applied by the pipeline, used by the node").isEqualTo(1);
    }
}
