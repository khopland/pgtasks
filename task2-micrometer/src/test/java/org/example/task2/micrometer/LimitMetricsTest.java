package org.example.task2.micrometer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import org.example.task2.Creation;
import org.example.task2.Insertion;
import org.example.task2.TopicRef;
import org.example.task2.engine.Rate;
import org.example.task2.engine.TaskEngine;
import org.example.task2.memory.InMemoryTaskStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;

/** Review 2026-11-21: rate limits had no metrics: not their values, not the time topics waited for them. */
class LimitMetricsTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    @Test
    void limits_show_their_values_and_how_long_topics_wait_for_them() {
        MeterRegistry meters = new SimpleMeterRegistry();
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore())
            .observer(new TaskObservations(ObservationRegistry.create(), meters))
            .globals(globals -> globals.limit("register", limit -> limit.rate(Rate.perSecond(20)).burst(1)))
            .configure("work", tasks -> tasks.eachTask(WORK, task -> { }, topic -> topic.limit("register").batchSize(10)))
            .build()) {
            new TaskMetrics(engine).bindTo(meters);
            assertThat(meters.get(TaskMetrics.LIMIT_RATE).tags("limit", "register", "kind", "configured").gauge().value()).isEqualTo(20);
            assertThat(meters.get(TaskMetrics.LIMIT_BURST).tags("limit", "register", "kind", "effective").gauge().value()).isEqualTo(1);

            engine.start();
            engine.tasks().independently().push(WORK, Insertion.APPEND, List.of(Creation.of("a", "1"), Creation.of("b", "2"), Creation.of("c", "3")));
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (meters.find(TaskObservations.LIMIT_WAIT).tags("topic", "work", "limit", "register").timer() == null) {
                assertThat(System.nanoTime()).as("the topic waited for its limit within 5 s").isLessThan(deadline);
                Thread.onSpinWait();
            }
            Timer waits = meters.get(TaskObservations.LIMIT_WAIT).timer();
            assertThat(waits.count()).isPositive();

            engine.limits().override("register", Rate.perSecond(50), null, null, "test", null);
            assertThat(meters.get(TaskMetrics.LIMIT_RATE).tags("limit", "register", "kind", "effective").gauge().value()).isEqualTo(50);
            assertThat(meters.get(TaskMetrics.LIMIT_RATE).tags("limit", "register", "kind", "configured").gauge().value()).isEqualTo(20);
        }
    }
}
