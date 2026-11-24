package io.github.khopland.pgtasks.ratelimit;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.extension.BatchListener;
import io.github.khopland.pgtasks.engine.extension.ClaimGate;
import io.github.khopland.pgtasks.engine.extension.Extension;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.ManualClock;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class RateLimitTest {

    @Test
    void a_bucket_spends_tokens_and_waits_for_the_refill() {
        ManualClock clock = ManualClock.startingNow();
        RateLimit.Buckets buckets = new RateLimit.Buckets();

        ClaimGate.Permit first = buckets.permit("work", 4, 2, clock);
        assertThat(first.waits()).isFalse();
        assertThat(first.maxTasks()).isEqualTo(2);
        buckets.taken("work", 2);

        ClaimGate.Permit held = buckets.permit("work", 4, 2, clock);
        assertThat(held.waits()).isTrue();
        assertThat(held.delay()).isEqualTo(Duration.ofMillis(250));

        clock.advance(Duration.ofMillis(100));
        assertThat(buckets.permit("work", 4, 2, clock).delay()).isEqualTo(Duration.ofMillis(150));
        clock.advance(Duration.ofMillis(150));
        assertThat(buckets.permit("work", 4, 2, clock).maxTasks()).isEqualTo(1);
        // A permit is only spent when the claim reports what it got.
        assertThat(buckets.permit("work", 4, 2, clock).maxTasks()).isEqualTo(1);
        buckets.taken("work", 1);
        clock.advance(Duration.ofMillis(500));
        assertThat(buckets.permit("work", 4, 2, clock).maxTasks()).isEqualTo(2);
    }

    @Test
    void buckets_are_separate_per_topic_and_follow_a_changed_burst() {
        ManualClock clock = ManualClock.startingNow();
        RateLimit.Buckets buckets = new RateLimit.Buckets();

        assertThat(buckets.permit("one", 4, 2, clock).maxTasks()).isEqualTo(2);
        assertThat(buckets.permit("two", 4, 2, clock).maxTasks()).isEqualTo(2);
        buckets.taken("one", 2);
        assertThat(buckets.permit("one", 4, 2, clock).waits()).isTrue();
        assertThat(buckets.permit("two", 4, 2, clock).maxTasks()).isEqualTo(2);

        assertThat(buckets.permit("one", 4, 4, clock).waits()).isTrue();
        clock.advance(Duration.ofSeconds(1));
        assertThat(buckets.permit("one", 4, 4, clock).maxTasks()).isEqualTo(4);
    }

    @Test
    void a_bucket_allows_a_task_a_time_even_below_one_per_second() {
        ManualClock clock = ManualClock.startingNow();
        RateLimit.Buckets buckets = new RateLimit.Buckets();

        assertThat(buckets.permit("slow", 0.5, 0, clock).maxTasks()).isEqualTo(1);
        buckets.taken("slow", 1);
        assertThat(buckets.permit("slow", 0.5, 0, clock).delay()).isEqualTo(Duration.ofSeconds(2));
        clock.advance(Duration.ofSeconds(2));
        assertThat(buckets.permit("slow", 0.5, 0, clock).maxTasks()).isEqualTo(1);
    }

    @Test
    void settings_take_finite_non_negative_numbers() {
        assertThat(RateLimit.PER_SECOND.parse(" 3.5 ")).isEqualTo(3.5);
        assertThatThrownBy(() -> RateLimit.PER_SECOND.parse("NaN"))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("finite");
        assertThatThrownBy(() -> RateLimit.PER_SECOND.parse("Infinity"))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("finite");
        assertThatThrownBy(() -> RateLimit.BURST.parse("-1"))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("non-negative");
    }

    @Test
    void the_extension_holds_an_engine_topic_back() throws Exception {
        TopicRef<String> limited = TopicRef.of("limited", String.class);
        List<BatchListener.Completed> seen = new CopyOnWriteArrayList<>();
        Extension listener = context -> context.batchListener((topic, tasks) -> seen.addAll(tasks));

        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore())
            .extension(new RateLimit(), listener)
            .configure("work", tasks -> tasks.eachTask(limited, task -> { }, options -> options.set(RateLimit.PER_SECOND, 20.0)))
            .build().start()) {
            long started = System.nanoTime();
            for (int index = 0; index < 30; index++) {
                engine.tasks().independently().push(limited, "t" + index, "x");
            }
            await("every task to finish", Duration.ofSeconds(10), () -> seen.size() == 30);
            Duration took = Duration.ofNanos(System.nanoTime() - started);

            assertThat(took).as("30 tasks at 20/s with a burst of 20 take at least half a second")
                .isGreaterThan(Duration.ofMillis(450));
            assertThat(seen).extracting(BatchListener.Completed::identifier).doesNotHaveDuplicates();
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
