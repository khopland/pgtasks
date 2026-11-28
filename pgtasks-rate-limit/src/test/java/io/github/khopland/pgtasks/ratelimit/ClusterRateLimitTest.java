package io.github.khopland.pgtasks.ratelimit;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    void an_idle_topic_spends_its_window_at_once_then_a_busy_one_gets_one_claim_at_a_time() {
        SlidingCounter counter = new SlidingCounter();
        SlidingCounter.Admission all = counter.admit(1_000, 1_000, 6, 100);
        assertThat(all.reserved()).isEqualTo(6);

        // No decay inside a saturated window: the next slot comes when the previous window starts to age.
        SlidingCounter.Admission held = counter.admit(1_100, 1_000, 6, 100);
        assertThat(held.waits()).isTrue();
        assertThat(held.delay()).isEqualTo(Duration.ofMillis(900));

        assertThat(counter.admit(2_000, 1_000, 6, 100).delay()).isEqualTo(Duration.ofMillis(167));
        assertThat(counter.admit(2_167, 1_000, 6, 100).reserved()).isEqualTo(1);
        assertThat(counter.admit(2_334, 1_000, 6, 100).reserved()).isEqualTo(1);

        // Once the busy windows are behind it, a whole window's allowance is available again.
        assertThat(counter.admit(4_000, 1_000, 6, 100).reserved()).isEqualTo(6);
    }

    @Test
    void a_reservation_takes_no_more_than_the_claim_wants() {
        SlidingCounter counter = new SlidingCounter();
        assertThat(counter.admit(0, 60_000, 1_000, 10).reserved()).isEqualTo(10);
        assertThat(counter.admit(0, 60_000, 1_000, 10).reserved()).isEqualTo(10);
        assertThat(counter.current()).isEqualTo(20);
    }

    @Test
    void an_unclaimed_reservation_is_returned_to_everyone() {
        SharedCounters counters = new SharedCounters(Instant.parse("2026-01-01T00:00:00Z").toEpochMilli());

        SlidingCounter.Admission one = counters.update("work", (state, now) -> state.admit(now, 1_000, 3, 10));
        assertThat(one.reserved()).isEqualTo(3);
        assertThat(counters.update("work", (state, now) -> state.admit(now, 1_000, 3, 10)).waits()).isTrue();

        counters.update("work", (state, now) -> {
            state.settle(one, 1, now);
            return true;
        });
        assertThat(counters.update("work", (state, now) -> state.admit(now, 1_000, 3, 10)).reserved()).isEqualTo(2);
    }

    @Test
    void what_goes_unused_after_the_window_moves_on_comes_off_the_window_it_was_counted_in() {
        SlidingCounter counter = new SlidingCounter();
        SlidingCounter.Admission early = counter.admit(999, 1_000, 100, 100);
        assertThat(early.reserved()).isEqualTo(100);
        // Another node moves the window on and claims 40 in the new one.
        SlidingCounter.Admission late = counter.admit(1_000, 1_000, 200, 40);
        assertThat(late.reserved()).isEqualTo(40);

        counter.settle(early, 10, 1_001);
        assertThat(counter.previous()).isEqualTo(10);
        assertThat(counter.current()).isEqualTo(40);

        // Two windows on, nothing it reserved still counts, so there is nothing to give back.
        SlidingCounter.Admission stale = counter.admit(1_500, 1_000, 200, 5);
        counter.admit(3_000, 1_000, 200, 1);
        counter.settle(stale, 0, 3_000);
        assertThat(counter.previous()).isZero();
        assertThat(counter.current()).isEqualTo(1);
    }

    @Test
    void settings_are_typed_and_validated() {
        assertThat(ClusterRateLimit.WINDOW.parse("PT10S")).isEqualTo(Duration.ofSeconds(10));
        assertThat(ClusterRateLimit.PER_WINDOW.parse(" 35 ")).isEqualTo(35L);
        assertThatThrownBy(() -> ClusterRateLimit.PER_WINDOW.parse("0.5")).isInstanceOf(NumberFormatException.class);
        assertThatThrownBy(() -> ClusterRateLimit.PER_WINDOW.parse("-1"))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("must not be negative");
        assertThatThrownBy(() -> ClusterRateLimit.WINDOW.checked(Duration.ofNanos(10)))
            .as("a window set in code gets the parser's check too").hasMessageContaining("cluster.window: a rate limit window must be at least a millisecond");
        assertThatThrownBy(() -> ClusterRateLimit.WINDOW.parse("PT0.0005S"))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("at least a millisecond");
    }

    @Test
    void the_extension_spends_the_shared_counter_for_its_topic_and_hands_back_what_it_did_not_claim() throws Exception {
        TopicRef<String> work = TopicRef.of("work", String.class);
        ManualClock clock = ManualClock.startingNow();
        SharedCounters counters = new SharedCounters(clock.instant().toEpochMilli());
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore(clock))
            .extension(new ClusterRateLimit(counters))
            .configure("work", tasks -> tasks.eachTask(work, task -> { },
                options -> options.set(ClusterRateLimit.PER_WINDOW, 100L).batchSize(10)))
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

    @Test
    void the_engine_does_not_start_when_the_counter_cannot_be_readied() {
        SlidingWindowStore unusable = new SlidingWindowStore() {
            @Override
            public <R> R update(String topic, Update<R> action) {
                throw new AssertionError("Not asked before it is ready");
            }

            @Override
            public void prepare() {
                throw new IllegalStateException("The rate_limit_window table is missing");
            }
        };
        TopicRef<String> work = TopicRef.of("work", String.class);
        TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore(ManualClock.startingNow()))
            .extension(new ClusterRateLimit(unusable))
            .configure("work", tasks -> tasks.eachTask(work, task -> { },
                options -> options.set(ClusterRateLimit.PER_WINDOW, 100L)))
            .build();
        try (engine) {
            assertThatThrownBy(engine::start).isInstanceOf(IllegalStateException.class).hasMessageContaining("rate_limit_window");
        }
    }

    @Test
    void a_counter_that_fails_holds_the_topic_back_instead_of_claiming_without_a_limit() throws Exception {
        CountDownLatch asked = new CountDownLatch(2);
        SlidingWindowStore failing = new SlidingWindowStore() {
            @Override
            public <R> R update(String topic, Update<R> action) {
                asked.countDown();
                throw new IllegalStateException("The database is gone");
            }
        };
        TopicRef<String> work = TopicRef.of("work", String.class);
        AtomicInteger handled = new AtomicInteger();
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore(ManualClock.startingNow()))
            .extension(new ClusterRateLimit(failing))
            .configure("work", tasks -> tasks.eachTask(work, task -> handled.incrementAndGet(),
                options -> options.set(ClusterRateLimit.PER_WINDOW, 100L)))
            .build().start()) {
            engine.tasks().independently().push(work, "a", "x");
            // Each claim asks the counter first, so a second question means the first claim was held back.
            assertThat(asked.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(handled).hasValue(0);
        }
    }

    /** A counter shared by every engine in one process at a fixed time; the Postgres capability does the same across JVMs. */
    private static final class SharedCounters implements SlidingWindowStore {
        private final Map<String, SlidingCounter> counters = new ConcurrentHashMap<>();
        private final long now;

        SharedCounters(long now) {
            this.now = now;
        }

        @Override
        public <R> R update(String topic, Update<R> action) {
            SlidingCounter counter = counters.computeIfAbsent(topic, ignored -> new SlidingCounter());
            synchronized (counter) {
                return action.apply(counter, now);
            }
        }
    }
}
