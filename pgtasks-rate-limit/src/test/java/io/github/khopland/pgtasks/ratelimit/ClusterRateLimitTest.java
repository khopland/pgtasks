package io.github.khopland.pgtasks.ratelimit;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.ManualClock;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class ClusterRateLimitTest {

    @Test
    void a_window_spends_its_burst_then_lets_one_claim_through_at_a_time() {
        SlidingCounter counter = new SlidingCounter();
        SlidingCounter.Admission burst = counter.admit(1_000, 1_000, 4, 2);
        assertThat(burst.reserved()).isEqualTo(6);
        counter.claimed(6, burst.reserved());

        // No decay inside a saturated window: the next slot comes when the previous window starts to age.
        SlidingCounter.Admission held = counter.admit(1_100, 1_000, 4, 2);
        assertThat(held.waits()).isTrue();
        assertThat(held.delay()).isEqualTo(Duration.ofMillis(900));

        assertThat(counter.admit(2_000, 1_000, 4, 2).delay()).isEqualTo(Duration.ofMillis(167));
        SlidingCounter.Admission first = counter.admit(2_167, 1_000, 4, 2);
        assertThat(first.reserved()).isEqualTo(1);
        counter.claimed(1, first.reserved());
        SlidingCounter.Admission second = counter.admit(2_334, 1_000, 4, 2);
        assertThat(second.reserved()).isEqualTo(1);
        counter.claimed(1, second.reserved());

        // Once the busy windows are behind it, a fresh burst is available again.
        assertThat(counter.admit(3_000, 1_000, 4, 2).reserved()).isEqualTo(4);
    }

    @Test
    void an_unclaimed_reservation_is_returned_to_everyone() {
        SharedCounters counters = new SharedCounters();
        long now = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();

        SlidingCounter.Admission one = counters.update("work", state -> state.admit(now, 1_000, 3, 0));
        assertThat(one.reserved()).isEqualTo(3);
        assertThat(counters.update("work", state -> state.admit(now, 1_000, 3, 0)).waits()).isTrue();

        counters.update("work", state -> {
            state.claimed(1, one.reserved());
            return null;
        });
        assertThat(counters.update("work", state -> state.admit(now, 1_000, 3, 0)).reserved()).isEqualTo(2);
    }

    @Test
    void settings_are_typed_and_validated() {
        assertThat(ClusterRateLimit.WINDOW.parse("PT10S")).isEqualTo(Duration.ofSeconds(10));
        assertThat(ClusterRateLimit.PER_WINDOW.parse(" 3.5 ")).isEqualTo(3.5);
        assertThatThrownBy(() -> ClusterRateLimit.PER_WINDOW.parse("NaN"))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("finite");
        assertThatThrownBy(() -> ClusterRateLimit.BURST.parse("-1"))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("non-negative");
    }

    @Test
    void the_extension_spends_the_shared_counter_for_its_topic() throws Exception {
        TopicRef<String> work = TopicRef.of("work", String.class);
        ManualClock clock = ManualClock.startingNow();
        SharedCounters counters = new SharedCounters();
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore(clock))
            .extension(new ClusterRateLimit(counters))
            .configure("work", tasks -> tasks.eachTask(work, task -> { },
                options -> options.set(ClusterRateLimit.PER_WINDOW, 3.0)
                    .set(ClusterRateLimit.BURST, 0.0)))
            .build().start()) {
            for (int index = 0; index < 3; index++) {
                engine.tasks().independently().push(work, "t" + index, "x");
            }
            TaskTester.of(engine).awaitIdle();
        }

        SlidingCounter state = counters.counters.get("work");
        assertThat(state).isNotNull();
        assertThat(state.current()).isEqualTo(3);
    }

    /** A counter shared by every engine in one process; the Postgres capability does the same across JVMs. */
    private static final class SharedCounters implements SlidingWindowStore {
        private final Map<String, SlidingCounter> counters = new ConcurrentHashMap<>();

        @Override
        public <R> R update(String topic, Function<SlidingCounter, R> action) {
            SlidingCounter counter = counters.computeIfAbsent(topic, ignored -> new SlidingCounter());
            synchronized (counter) {
                return action.apply(counter);
            }
        }
    }
}
