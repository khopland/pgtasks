package io.github.khopland.pgtasks.micrometer;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.engine.extension.ClaimGate;
import io.github.khopland.pgtasks.engine.extension.Extension;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.ManualClock;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** {@value TaskMetrics#HELD}: how long this node's claim gates have held a topic back. */
class HeldMetricsTest {

    private static final TopicRef<String> HELD = TopicRef.of("held", String.class);
    private static final TopicRef<String> FREE = TopicRef.of("free", String.class);

    @Test
    void a_held_topic_counts_the_seconds_and_a_free_one_reads_zero() throws Exception {
        AtomicBoolean holding = new AtomicBoolean(true);
        Extension pause = Extension.of("pause", context -> context.claimGate((topic, wanted) ->
            holding.get() && topic.name().equals(HELD.name()) ? ClaimGate.Permit.waitFor(Duration.ofMinutes(1)) : ClaimGate.Permit.ANY));
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore(ManualClock.startingNow())).extension(pause)
            .configure("work", tasks -> tasks.eachTask(HELD, task -> { }).eachTask(FREE, task -> { }))
            .build().start()) {
            TaskTester tester = TaskTester.of(engine);
            // The leader's count snapshot makes the topics known to the gauges.
            tester.advance(Duration.ZERO);
            engine.tasks().independently().push(HELD, "a", "x");
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (engine.heldTopics().isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            Thread.sleep(50);
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            new TaskMetrics(engine).bindTo(registry);

            assertThat(registry.get(TaskMetrics.HELD).tag("topic", "held").gauge().value()).isPositive();
            assertThat(registry.get(TaskMetrics.HELD).tag("topic", "free").gauge().value()).isZero();

            holding.set(false);
            engine.wake("held");
            tester.awaitIdle();
            assertThat(registry.get(TaskMetrics.HELD).tag("topic", "held").gauge().value()).isZero();
        }
    }
}
