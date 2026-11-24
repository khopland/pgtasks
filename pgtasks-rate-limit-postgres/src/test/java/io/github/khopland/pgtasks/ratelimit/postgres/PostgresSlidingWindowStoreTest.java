package io.github.khopland.pgtasks.ratelimit.postgres;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.postgres.TaskSchema;
import io.github.khopland.pgtasks.ratelimit.ClusterRateLimit;
import io.github.khopland.pgtasks.ratelimit.SlidingCounter;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

final class PostgresSlidingWindowStoreTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));

    @Test
    void nodes_take_turns_on_one_counter(PostgresDatabase database) throws Exception {
        PostgresSlidingWindowStore first = new PostgresSlidingWindowStore(new PostgresTaskStore(database.dataSource()));
        PostgresSlidingWindowStore second = new PostgresSlidingWindowStore(new PostgresTaskStore(database.dataSource()));
        long now = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int index = 0; index < 40; index++) {
                PostgresSlidingWindowStore store = index % 2 == 0 ? first : second;
                futures.add(pool.submit(() -> store.update("work", state -> {
                    SlidingCounter.Admission admitted = state.admit(now, 60_000, 1_000_000, 0);
                    state.claimed(1, admitted.reserved());
                    return null;
                })));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(first.update("work", SlidingCounter::current)).isEqualTo(40);
    }

    @Test
    void the_extension_finds_the_capability_from_its_store(PostgresDatabase database) throws Exception {
        TopicRef<String> work = TopicRef.of("work", String.class);
        try (TaskEngine engine = TaskEngine.builder().dataSource(database.dataSource())
            .extension(new ClusterRateLimit())
            .configure("work", tasks -> tasks.eachTask(work, task -> { },
                options -> options.set(ClusterRateLimit.PER_WINDOW, 10.0)
                    .set(ClusterRateLimit.WINDOW, java.time.Duration.ofSeconds(10))))
            .build().start()) {
            engine.tasks().independently().push(work, "a", "x");
            TaskTester.of(engine).awaitIdle();
        }
    }
}
