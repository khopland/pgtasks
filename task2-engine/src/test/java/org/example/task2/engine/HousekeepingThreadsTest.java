package org.example.task2.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Period;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.example.task2.Decision;
import org.example.task2.Outcome;
import org.example.task2.TopicRef;
import org.example.task2.memory.InMemoryTaskStore;
import org.example.task2.spi.TaskStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Review F6: one thread ran all housekeeping, so a long purge stopped the heartbeat and cost the lease. */
class HousekeepingThreadsTest {

    @Test
    @DisplayName("A purge that takes long holds up neither the heartbeat nor the lease")
    void a_slow_purge_doesnt_stop_the_heartbeat() throws Exception {
        CountDownLatch purging = new CountDownLatch(1), release = new CountDownLatch(1);
        InMemoryTaskStore memory = new InMemoryTaskStore();
        TaskStore store = (TaskStore) Proxy.newProxyInstance(TaskStore.class.getClassLoader(), new Class<?>[] {TaskStore.class},
            (proxy, method, arguments) -> {
                // A slow database, for the retention thread only. Blocking inside a transaction
                // would block everyone: the in-memory store has one lock for all of them.
                if (method.getName().equals("begin") && Thread.currentThread().getName().startsWith("task2-retention")) {
                    purging.countDown();
                    release.await();
                }
                try {
                    return method.invoke(memory, arguments);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
        try (TaskEngine engine = TaskEngine.builder()
            .store(store)
            .globals(globals -> globals.heartbeat(Duration.ofMillis(50), Duration.ofSeconds(30))
                .housekeeping(housekeeping -> housekeeping.purgeEvery(Duration.ofMillis(10))))
            .configure("work", tasks -> tasks.topic(TopicRef.of("work", String.class), batch -> batch.decide(task -> Decision.success()),
                topic -> topic.retain(Outcome.SUCCEEDED, Period.ofDays(1))))
            .build()
            .start()) {
            assertThat(purging.await(5, TimeUnit.SECONDS)).as("the purge started").isTrue();

            // Four heartbeats make the lease term: a stuck heartbeat would lose it well within this.
            Thread.sleep(1_000);

            assertThat(engine.leadsHousekeeping()).isTrue();
            assertThat(engine.health().heartbeatOnTime()).isTrue();
            assertThat(engine.health().heartbeatAge()).isLessThan(Duration.ofMillis(200));
            release.countDown();
        } finally {
            release.countDown();
        }
    }
}
