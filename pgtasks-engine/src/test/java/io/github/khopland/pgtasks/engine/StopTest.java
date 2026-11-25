package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Review 2026-11-25: a stop waited for each topic's cancelled handlers in turn, so every topic whose
 * handlers ignored cancellation added its own wait. With a handful of such topics the stop ran past
 * the 30 s that Kubernetes gives a pod by default, and the process was killed before it handed
 * its tasks back or left the cluster.
 */
class StopTest {

    private final MemoryEngines engines = new MemoryEngines();
    private volatile boolean released;

    @AfterEach
    void tearDown() {
        released = true;
        engines.close();
    }

    @Test
    void handlers_ignoring_cancellation_in_many_topics_cost_one_wait_together() throws Exception {
        int count = 5;
        CountDownLatch running = new CountDownLatch(count);
        List<Topic<?>> topics = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            topics.add(Topic.of(TopicRef.of("topic-" + index, String.class), batch -> batch.decide(task -> {
                running.countDown();
                while (!released) {
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ignored) {
                        // Ignores cancellation, as a handler stuck in a call that can't be interrupted does.
                    }
                }
                return Decision.success();
            })));
        }
        TaskEngine engine = engines.start("node-a", topics);
        for (int index = 0; index < count; index++) {
            engine.tasks().independently().push(TopicRef.of("topic-" + index, String.class), "a", "x");
        }
        assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();

        long started = System.nanoTime();
        engine.stop(Duration.ofMillis(200));
        Duration took = Duration.ofNanos(System.nanoTime() - started);

        assertThat(took).as("one patience for the five topics, not one each").isLessThan(TopicWorker.PATIENCE.plusSeconds(1));
    }
}
