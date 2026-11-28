package io.github.khopland.pgtasks.pause.postgres;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

import io.github.khopland.pgtasks.SchemaMode;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.EngineHealth;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.pause.Pause;
import io.github.khopland.pgtasks.pause.PauseStore;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.postgres.TaskSchema;
import io.github.khopland.pgtasks.postgres.TaskSchemaException;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

final class PostgresPauseStoreTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));

    private static final TopicRef<String> ASSESSMENT = TopicRef.of("assessment", String.class);
    private static final Duration SOON = Duration.ofSeconds(1);

    private final Set<String> processed = ConcurrentHashMap.newKeySet();
    private final List<TaskEngine> engines = new ArrayList<>();

    @AfterEach
    void stop() {
        engines.forEach(TaskEngine::close);
    }

    private TaskEngine node(PostgresDatabase database, String name, Pause pause, boolean runs) {
        var builder = TaskEngine.builder().dataSource(database.dataSource()).node(name).extension(pause)
            // An hour between heartbeats: only the notification carries a pause or a resume here.
            .heartbeat(Duration.ofHours(1), Duration.ofHours(10));
        if (runs) {
            builder.configure("assessment", tasks -> tasks.eachTask(ASSESSMENT, task -> processed.add(name + "/" + task.payload()),
                options -> options.pollInterval(Duration.ofMillis(50))));
        }
        TaskEngine engine = builder.build().start();
        engines.add(engine);
        return engine;
    }

    /** In the chaos run, a pause reached all three nodes within 5 ms; here, within a second. */
    @Test
    void pauses_and_resumes_reach_every_node_within_a_second(PostgresDatabase database) throws Exception {
        Pause operator = new Pause();
        TaskEngine runner = node(database, "node-a", operator, false);
        node(database, "node-b", new Pause(), true);
        node(database, "node-c", new Pause(), true);
        await("every node listening", Duration.ofSeconds(5), () -> engines.stream().allMatch(PostgresPauseStoreTest::listening));
        runner.tasks().independently().push(ASSESSMENT, "a", "a");
        await("its task to run", SOON, () -> processed.stream().anyMatch(each -> each.endsWith("/a")));

        assertThat(operator.pause(ASSESSMENT.name())).isTrue();
        // The other nodes hear it once it commits; give the message a moment before pushing.
        Thread.sleep(200);
        for (int index = 0; index < 5; index++) {
            runner.tasks().independently().push(ASSESSMENT, "b" + index, "b" + index);
        }
        Thread.sleep(300);
        assertThat(processed).allMatch(each -> each.endsWith("/a"));

        assertThat(operator.resume(ASSESSMENT.name())).isTrue();
        await("the waiting tasks to run after the resume", SOON,
            () -> processed.stream().filter(each -> each.contains("/b")).count() == 5);
    }

    @Test
    void the_table_is_the_extensions_own_and_pauses_are_stamped_by_the_database(PostgresDatabase database) throws Exception {
        PostgresTaskStore tasks = new PostgresTaskStore(database.dataSource());
        PostgresPauseStore store = new PostgresPauseStore(tasks);
        try (StoreTransaction transaction = tasks.begin()) {
            assertThat(store.pause(transaction, "work")).isTrue();
            assertThat(store.pause(transaction, "work")).isFalse();
            assertThat(store.list(transaction)).singleElement().satisfies(paused -> {
                assertThat(paused.topic()).isEqualTo("work");
                assertThat(paused.pausedAt().toEpochMilli()).isCloseTo(System.currentTimeMillis(), within(60_000L));
            });
            transaction.commit();
        }
        try (StoreTransaction transaction = tasks.begin()) {
            assertThat(store.resume(transaction, "work")).isTrue();
            assertThat(store.resume(transaction, "work")).isFalse();
            assertThat(store.list(transaction)).isEmpty();
            transaction.commit();
        }
        assertThat(database.rows("SELECT script FROM pgtasks.pause_schema_history"))
            .extracting(row -> row.get(0)).containsExactly("V001__topic_pause.sql");
    }

    @Test
    void validate_mode_requires_the_extensions_own_table(PostgresDatabase database) {
        PostgresPauseStore store = new PostgresPauseStore(new PostgresTaskStore(database.dataSource(), SchemaMode.VALIDATE));
        assertThatThrownBy(store::prepare)
            .isInstanceOf(IllegalStateException.class)
            .hasCauseInstanceOf(TaskSchemaException.class)
            .satisfies(e -> assertThat(e.getCause()).hasMessageContaining("pgtasks.topic_pause table is missing"));
        assertThatThrownBy(() -> TaskEngine.builder().dataSource(database.dataSource()).schemaMode(SchemaMode.VALIDATE)
            .extension(new Pause()).build().start())
            .hasStackTraceContaining("pgtasks.topic_pause table is missing");
    }

    @Test
    void the_extension_finds_the_capability_from_its_store(PostgresDatabase database) throws Exception {
        Pause pause = new Pause();
        TaskEngine engine = node(database, "node-a", pause, true);
        pause.pause(ASSESSMENT.name());
        assertThat(pause.paused()).extracting(PauseStore.Paused::topic).containsExactly(ASSESSMENT.name());
        assertThat(database.count("SELECT count(*) FROM pgtasks.topic_pause WHERE topic = ?", ASSESSMENT.name())).isEqualTo(1);
        pause.resume(ASSESSMENT.name());
        engine.tasks().independently().push(ASSESSMENT, "x", "x");
        await("the task to run", SOON, () -> processed.contains("node-a/x"));
    }

    private static boolean listening(TaskEngine engine) {
        try {
            return engine.health().notifications() == EngineHealth.Notifications.CONNECTED;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void await(String what, Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting for " + what);
            }
            Thread.sleep(10);
        }
    }
}
