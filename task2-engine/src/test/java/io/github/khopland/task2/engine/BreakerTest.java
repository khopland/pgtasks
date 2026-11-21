package io.github.khopland.task2.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import io.github.khopland.task2.Creation;
import io.github.khopland.task2.Decision;
import io.github.khopland.task2.Insertion;
import io.github.khopland.task2.TopicRef;
import io.github.khopland.task2.memory.InMemoryTaskStore;
import io.github.khopland.task2.spi.ManualClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The breaker: a topic whose tasks mostly fail stops claiming and probes, one task at a time. */
class BreakerTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    private final ManualClock clock = ManualClock.startingNow();
    private final AtomicBoolean down = new AtomicBoolean(true);
    private final AtomicInteger runs = new AtomicInteger();
    private TaskEngine engine;

    @AfterEach
    void stop() {
        if (engine != null) {
            engine.close();
        }
    }

    private TaskTester start(Retry retry) throws Exception {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                runs.incrementAndGet();
                return down.get() || task.payload().equals("poison") ? Decision.failed("downstream refused") : Decision.success();
            }), topic -> topic.batchSize(10).retry(retry).breaker(Breaker.standard().opensAt(0.8, 10))))
            .build()
            .start();
        return TaskTester.of(engine);
    }

    /** Review 2026-11-21: bad input the policy dead-letters at once opened the breaker, and the topic crawled at a task per probe. */
    @Test
    void bad_input_that_goes_straight_to_the_dead_letters_doesnt_open_the_breaker() throws Exception {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                runs.incrementAndGet();
                if (task.payload().equals("bad")) {
                    throw new IllegalArgumentException("invalid row");
                }
                return Decision.success();
            }), topic -> topic.batchSize(10).retry(Retry.standard().deadLetterAt(IllegalArgumentException.class))
                .breaker(Breaker.standard().opensAt(0.8, 10))))
            .build()
            .start();
        TaskTester tester = TaskTester.of(engine);
        engine.tasks().independently().push(WORK, Insertion.APPEND,
            IntStream.range(0, 200).mapToObj(index -> Creation.of("t" + index, index % 10 < 8 ? "bad" : "good")).toList());

        tester.awaitIdle();

        assertThat(engine.health().openBreakers()).isEmpty();
        assertThat(runs).as("every task ran once, without waiting for probes").hasValue(200);
        assertThat(tester.counts(WORK).failed()).isEqualTo(160);
    }

    @Test
    @DisplayName("During an outage the topic stops claiming, probes without using attempts, and resumes once a probe succeeds")
    void an_outage_opens_the_breaker_and_a_successful_probe_closes_it() throws Exception {
        TaskTester tester = start(Retry.standard());
        engine.tasks().independently().push(WORK, Insertion.APPEND,
            IntStream.range(0, 20).mapToObj(index -> Creation.of("t" + index, "payload")).toList());

        tester.awaitIdle();
        assertThat(engine.health().openBreakers()).containsExactly("work");
        assertThat(engine.admin().topics()).singleElement().extracting(io.github.khopland.task2.admin.TopicStatus::breakerOpen).isEqualTo(true);
        int beforeWaiting = runs.get();
        assertThat(beforeWaiting).as("the first batch opened it; the second ran beside it").isBetween(10, 20);

        // The retries are due after 10 s, but the probe waits for 30 s: nothing runs meanwhile.
        tester.advance(Duration.ofSeconds(15));
        tester.awaitIdle();
        assertThat(runs).hasValue(beforeWaiting);

        // A probe: one task, still failing, and its attempt doesn't count.
        tester.advance(Duration.ofSeconds(20));
        tester.awaitIdle();
        assertThat(runs).hasValue(beforeWaiting + 1);
        assertThat(engine.health().openBreakers()).containsExactly("work");

        down.set(false);
        tester.drain(WORK);

        assertThat(engine.health().openBreakers()).isEmpty();
        assertThat(tester.counts(WORK).total()).isZero();
        long counted = IntStream.range(0, 20).mapToLong(index -> unchecked(() -> engine.admin().find(index + 1).orElseThrow().attempt())).max().orElseThrow();
        assertThat(counted).as("only the failures before the breaker opened counted").isLessThanOrEqualTo(2);
    }

    @Test
    @DisplayName("One task that keeps failing doesn't open the breaker; it reaches the dead letters")
    void a_poison_task_alone_doesnt_open_it() throws Exception {
        down.set(false);
        TaskTester tester = start(Retry.standard().attempts(30));
        long poison = engine.tasks().independently().push(WORK, "poison", "poison").id();

        tester.drain(WORK);

        assertThat(runs).hasValue(30);
        assertThat(engine.health().openBreakers()).isEmpty();
        assertThat(engine.admin().find(poison).orElseThrow().state()).isEqualTo("failed");
    }

    private static <V> V unchecked(java.util.concurrent.Callable<V> call) {
        try {
            return call.call();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
