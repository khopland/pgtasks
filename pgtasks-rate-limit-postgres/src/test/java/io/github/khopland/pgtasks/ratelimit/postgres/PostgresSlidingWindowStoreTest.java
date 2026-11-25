package io.github.khopland.pgtasks.ratelimit.postgres;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.github.khopland.pgtasks.SchemaMode;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.postgres.TaskSchema;
import io.github.khopland.pgtasks.postgres.TaskSchemaException;
import io.github.khopland.pgtasks.ratelimit.ClusterRateLimit;
import io.github.khopland.pgtasks.ratelimit.SlidingCounter;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

final class PostgresSlidingWindowStoreTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));

    @Test
    void nodes_take_turns_on_one_counter(PostgresDatabase database) throws Exception {
        PostgresSlidingWindowStore first = new PostgresSlidingWindowStore(new PostgresTaskStore(database.dataSource()));
        PostgresSlidingWindowStore second = new PostgresSlidingWindowStore(new PostgresTaskStore(database.dataSource()));
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int index = 0; index < 40; index++) {
                PostgresSlidingWindowStore store = index % 2 == 0 ? first : second;
                futures.add(pool.submit(() -> store.update("work", (state, now) -> {
                    SlidingCounter.Admission admitted = state.admit(now, 3_600_000, 1_000_000, 5);
                    state.settle(admitted, 1, now);
                    return true;
                })));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdownNow();
        }
        // An hour-long window, so the test only crosses a boundary if it runs exactly on the hour.
        assertThat(first.<Long>update("work", (state, now) -> state.current() + state.previous())).isEqualTo(40);
        assertThat(database.rows("SELECT script FROM pgtasks.rate_limit_schema_history"))
            .extracting(row -> row.get(0)).containsExactly("V001__rate_limit_window.sql");
    }

    @Test
    void a_new_counter_starts_on_the_databases_current_window_and_an_unchanged_one_is_not_written(PostgresDatabase database) {
        PostgresSlidingWindowStore store = new PostgresSlidingWindowStore(new PostgresTaskStore(database.dataSource()));
        long windowStart = store.<Long>update("clocked", (state, now) -> {
            state.admit(now, 1_000, 5, 1);
            return state.windowStart();
        });
        assertThat(windowStart).isCloseTo(System.currentTimeMillis(), within(60_000L)).isEqualTo(windowStart / 1_000 * 1_000);
        assertThat(store.<Boolean>update("clocked", (state, now) -> state.changed())).isFalse();
    }

    @Test
    void validate_mode_requires_the_extensions_own_table(PostgresDatabase database) {
        PostgresSlidingWindowStore store = new PostgresSlidingWindowStore(
            new PostgresTaskStore(database.dataSource(), SchemaMode.VALIDATE));
        assertThatThrownBy(() -> store.<Long>update("work", (state, now) -> state.current()))
            .isInstanceOf(IllegalStateException.class)
            .hasCauseInstanceOf(TaskSchemaException.class)
            .satisfies(e -> assertThat(e.getCause()).hasMessageContaining("pgtasks.rate_limit_window table is missing"));
    }

    @Test
    void the_extension_finds_the_capability_from_its_store(PostgresDatabase database) throws Exception {
        TopicRef<String> work = TopicRef.of("work", String.class);
        try (TaskEngine engine = TaskEngine.builder().dataSource(database.dataSource())
            .extension(new ClusterRateLimit())
            .configure("work", tasks -> tasks.eachTask(work, task -> { },
                options -> options.set(ClusterRateLimit.PER_WINDOW, 10L)
                    .set(ClusterRateLimit.WINDOW, java.time.Duration.ofSeconds(10))))
            .build().start()) {
            engine.tasks().independently().push(work, "a", "x");
            TaskTester.of(engine).awaitIdle();
        }
    }
}
