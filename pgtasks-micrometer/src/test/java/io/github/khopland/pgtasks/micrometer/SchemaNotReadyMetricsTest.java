package io.github.khopland.pgtasks.micrometer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.TaskStore;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Found 2026-11-22: bound before the engine had migrated an empty database, the metrics read the
 * snapshots at once and logged "relation pgtasks.topic_count does not exist".
 */
class SchemaNotReadyMetricsTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    @Test
    void nothing_is_read_before_the_engine_has_prepared_the_store() {
        AtomicBoolean prepared = new AtomicBoolean();
        AtomicInteger reads = new AtomicInteger();
        TaskStore memory = new InMemoryTaskStore();
        // Like Postgres before the migration: reading the snapshots fails until prepare() has run.
        TaskStore store = (TaskStore) Proxy.newProxyInstance(TaskStore.class.getClassLoader(), new Class<?>[] {TaskStore.class},
            (proxy, method, arguments) -> {
                switch (method.getName()) {
                    case "prepare" -> prepared.set(true);
                    case "snapshots" -> {
                        reads.incrementAndGet();
                        if (!prepared.get()) {
                            throw new SQLException("relation \"pgtasks.topic_count\" does not exist");
                        }
                    }
                    default -> { }
                }
                try {
                    return method.invoke(memory, arguments);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
        MeterRegistry meters = new SimpleMeterRegistry();
        try (TaskEngine engine = TaskEngine.builder().store(store)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> { }))
            .build()) {
            new TaskMetrics(engine).bindTo(meters);
            assertThat(meters.get(TaskMetrics.TOPICS).gauge().value()).isZero();
            assertThat(reads).hasValue(0);

            engine.prepare();
            // The first scrape after it reads at once, not after the refresh interval.
            meters.get(TaskMetrics.TOPICS).gauge().value();
            assertThat(reads).hasValue(1);
        }
    }
}
