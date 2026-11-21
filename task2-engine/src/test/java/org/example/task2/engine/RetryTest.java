package org.example.task2.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;
import java.util.random.RandomGenerator;

import org.example.task2.Decision;
import org.example.task2.memory.InMemoryTaskStore;
import org.junit.jupiter.api.Test;

class RetryTest {

    private static final RandomGenerator NO_JITTER = new RandomGenerator() {
        @Override
        public long nextLong() {
            return 0;
        }

        @Override
        public double nextDouble() {
            return 1;
        }
    };

    @Test
    void the_standard_policy_ramps_to_half_an_hour_and_gives_up_after_a_week() {
        Retry retry = Retry.standard();
        assertThat(retry.backoff(1, NO_JITTER)).isEqualTo(Duration.ofSeconds(10));
        assertThat(retry.backoff(8, NO_JITTER)).isEqualTo(Duration.ofSeconds(1280));
        assertThat(retry.backoff(9, NO_JITTER)).isEqualTo(Duration.ofMinutes(30));
        // 10 s + ... + 1,280 s = 2,550 s ramping, then (7 days - 2,550 s) / 30 min = 334 more.
        assertThat(retry.maxAttempts()).isEqualTo(343);
        assertThat(retry.describe()).isEqualTo("for 7 days (343 attempts), from PT10S x2.0 up to PT30M");
    }

    @Test
    void a_budget_forever_or_a_number_of_attempts() {
        Retry ramp = Retry.exponential(Duration.ofSeconds(10), Duration.ofMinutes(30));
        assertThat(ramp.giveUpAfter(Duration.ofHours(1)).maxAttempts()).as("2,550 s ramping, then 1,050 s left: no room for another").isEqualTo(9);
        assertThat(ramp.forever().isForever()).isTrue();
        assertThat(ramp.forever().allowsAnotherAfter(1_000_000)).isTrue();
        assertThat(ramp.forever().describe()).isEqualTo("forever, from PT10S x2.0 up to PT30M");
        assertThat(ramp.attempts(5).describe()).isEqualTo("5 attempts, from PT10S x2.0 up to PT30M");
        assertThat(Retry.none().allowsAnotherAfter(1)).isFalse();
        // A tiny backoff and a long budget: counted at once, not one by one.
        assertThat(Retry.exponential(Duration.ofMillis(1), Duration.ofMillis(1)).giveUpAfter(Duration.ofDays(1)).maxAttempts())
            .isEqualTo(86_400_001);
        assertThat(Retry.exponential(Duration.ofMillis(1), Duration.ofMillis(1)).giveUpAfter(Duration.ofDays(365)).maxAttempts())
            .as("capped below forever").isEqualTo(Retry.FOREVER - 1);
        assertThatThrownBy(() -> Retry.none().giveUpAfter(Duration.ofDays(1))).hasMessageContaining("backoff above zero");
    }

    @Test
    void overrides_change_parts_of_the_policy_in_any_order() {
        for (Map<String, String> overrides : java.util.List.of(
            java.util.Map.of("topics.work.retry.first-backoff", "1s", "topics.work.retry.max-backoff", "1m", "topics.work.retry.give-up-after", "1h"),
            new java.util.TreeMap<>(java.util.Map.of("topics.work.retry.give-up-after", "1h", "topics.work.retry.max-backoff", "1m",
                "topics.work.retry.first-backoff", "1s")))) {
            TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore())
                .configure("work", tasks -> tasks.topic("work", String.class, batch -> batch.decide(task -> Decision.success())))
                .overrides(overrides)
                .build();
            Retry retry = engine.topics().getFirst().settings().retry();
            assertThat(retry.initial()).isEqualTo(Duration.ofSeconds(1));
            assertThat(retry.max()).isEqualTo(Duration.ofMinutes(1));
            // 1+2+4+8+16+32 = 63 s ramping, then (3,600 - 63) / 60 = 58 more: 7 + 58.
            assertThat(retry.maxAttempts()).isEqualTo(65);
        }
        TaskEngine never = TaskEngine.builder().store(new InMemoryTaskStore())
            .configure("work", tasks -> tasks.topic("work", String.class, batch -> batch.decide(task -> Decision.success())))
            .overrides(Map.of("topics.work.retry.give-up-after", "never"))
            .build();
        assertThat(never.topics().getFirst().settings().retry().isForever()).isTrue();
    }
}
