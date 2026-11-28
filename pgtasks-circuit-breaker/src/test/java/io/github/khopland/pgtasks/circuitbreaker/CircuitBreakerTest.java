package io.github.khopland.pgtasks.circuitbreaker;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.engine.EffectiveSettings;
import io.github.khopland.pgtasks.engine.Retry;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskObserver;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.engine.extension.BatchListener;
import io.github.khopland.pgtasks.engine.extension.OperatorView;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.ManualClock;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The breaker: a topic whose tasks mostly fail stops claiming and probes, one task at a time. */
final class CircuitBreakerTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    private final ManualClock clock = ManualClock.startingNow();
    private final AtomicBoolean down = new AtomicBoolean(true);
    private final AtomicInteger runs = new AtomicInteger();
    private final CircuitBreaker breaker = new CircuitBreaker();
    private TaskEngine engine;

    @AfterEach
    void stop() {
        if (engine != null) {
            engine.close();
        }
    }

    private TaskTester start(Retry retry) {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .extension(breaker)
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                runs.incrementAndGet();
                return down.get() || task.payload().equals("poison") ? Decision.failed("downstream refused") : Decision.success();
            }), topic -> topic.batchSize(10).retry(retry).set(CircuitBreaker.WINDOW, 10)))
            .build()
            .start();
        return TaskTester.of(engine);
    }

    /** Review 2026-11-21: bad input the policy dead-letters at once opened the breaker, and the topic crawled at a task per probe. */
    @Test
    void bad_input_that_goes_straight_to_the_dead_letters_doesnt_open_the_breaker() {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .extension(breaker)
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                runs.incrementAndGet();
                if (task.payload().equals("bad")) {
                    throw new IllegalArgumentException("invalid row");
                }
                return Decision.success();
            }), topic -> topic.batchSize(10).retry(Retry.standard().deadLetterAt(IllegalArgumentException.class)).set(CircuitBreaker.WINDOW, 10)))
            .build()
            .start();
        TaskTester tester = TaskTester.of(engine);
        engine.tasks().independently().push(WORK, Insertion.APPEND,
            IntStream.range(0, 200).mapToObj(index -> Creation.of("t" + index, index % 10 < 8 ? "bad" : "good")).toList());

        tester.awaitIdle();

        assertThat(breaker.openTopics()).isEmpty();
        assertThat(runs).as("every task ran once, without waiting for probes").hasValue(200);
        assertThat(tester.counts(WORK).deadLetters()).isEqualTo(160);
    }

    @Test
    @DisplayName("During an outage the topic stops claiming, probes without using attempts, and resumes once a probe succeeds")
    void an_outage_opens_the_breaker_and_a_successful_probe_closes_it() {
        TaskTester tester = start(Retry.standard());
        engine.tasks().independently().push(WORK, Insertion.APPEND,
            IntStream.range(0, 20).mapToObj(index -> Creation.of("t" + index, "payload")).toList());

        tester.awaitIdle();
        assertThat(breaker.openTopics()).containsExactly("work");
        assertThat(breaker.isOpen(WORK)).isTrue();
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
        assertThat(breaker.isOpen(WORK)).isTrue();

        down.set(false);
        tester.drain(WORK);

        assertThat(breaker.openTopics()).isEmpty();
        assertThat(tester.counts(WORK).total()).isZero();
        int counted = IntStream.rangeClosed(1, 20).map(id -> tester.task(id).attempt()).max().orElseThrow();
        assertThat(counted).as("only the failures before the breaker opened counted").isLessThanOrEqualTo(2);
    }

    @Test
    @DisplayName("One task that keeps failing doesn't open the breaker; it reaches the dead letters")
    void a_poison_task_alone_doesnt_open_it() {
        down.set(false);
        TaskTester tester = start(Retry.standard().attempts(30));
        long poison = engine.tasks().independently().push(WORK, "poison", "poison").id();

        tester.drain(WORK);

        assertThat(runs).hasValue(30);
        assertThat(breaker.openTopics()).isEmpty();
        assertThat(tester.task(poison).state()).isEqualTo("dead_letter");
    }

    /** Review 2026-11-23: a probe out of attempts was retried as an uncounted probe failure, never dead-lettered. */
    @Test
    void a_probe_out_of_attempts_is_dead_lettered() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore(clock);
        engine = TaskEngine.builder()
            .store(store)
            .extension(breaker)
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                runs.incrementAndGet();
                return Decision.failed("downstream refused");
            }), topic -> topic.batchSize(10).retry(Retry.standard().attempts(2)).set(CircuitBreaker.WINDOW, 10)))
            .build()
            .prepare();
        // A task that used both its attempts on another node: its next claim finds it out of
        // attempts, as when the last one expired with its node. Due once the breaker is open.
        long spent = engine.tasks().independently().push(WORK, "spent", "payload").id();
        int topicId = transact(store, transaction -> store.findTopic(transaction, "work").orElseThrow());
        int other = transact(store, transaction -> store.registerNode(transaction, "other", null));
        for (int attempt = 1; attempt <= 2; attempt++) {
            Duration delay = Duration.ofSeconds(attempt == 1 ? 1 : 60);
            TaskStore.ClaimedTask claim = transact(store, transaction -> store.claim(transaction,
                new TaskStore.ClaimRequest(topicId, other, Ordering.FIFO, 1))).tasks().getFirst();
            transact(store, transaction -> {
                store.complete(transaction, topicId, other, List.of(new TaskStore.Completion(claim.id(), claim.attempt(), claim.claimedAt(),
                    new TaskStore.Resolution.Waiting(TaskStore.WaitState.RETRYING, delay, true, null))));
                return null;
            });
            if (attempt == 1) {
                clock.advance(Duration.ofSeconds(2));
                transact(store, transaction -> store.promoteDue(transaction, 10));
            }
        }
        assertThat(engine.admin().find(spent).orElseThrow().attempt()).isEqualTo(2);

        engine.start();
        TaskTester tester = TaskTester.of(engine);
        engine.tasks().independently().push(WORK, Insertion.APPEND,
            IntStream.range(0, 10).mapToObj(index -> Creation.of("t" + index, "payload")).toList());
        tester.awaitIdle();
        assertThat(breaker.isOpen(WORK)).isTrue();
        int ranBefore = runs.get();

        // Probes, one task at a time, until the spent task's turn has come.
        for (int probe = 0; probe < 15 && !tester.task(spent).state().equals("dead_letter"); probe++) {
            tester.advance(Duration.ofSeconds(31));
            tester.awaitIdle();
        }

        TaskView task = tester.task(spent);
        assertThat(task.state()).as("dead-lettered, not retried as a probe").isEqualTo("dead_letter");
        assertThat(task.attempt()).isEqualTo(3);
        assertThat(runs.get() - ranBefore).as("the handler never ran it").isLessThan(15);
        assertThat(breaker.isOpen(WORK)).isTrue();
    }

    @Test
    void a_topic_with_the_breaker_turned_off_keeps_claiming_whatever_fails() {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .extension(breaker)
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                runs.incrementAndGet();
                return Decision.failed("downstream refused");
            }), topic -> topic.batchSize(10).retry(Retry.none()).set(CircuitBreaker.WINDOW, 10).set(CircuitBreaker.ENABLED, false)))
            .build()
            .start();
        TaskTester tester = TaskTester.of(engine);
        engine.tasks().independently().push(WORK, Insertion.APPEND,
            IntStream.range(0, 50).mapToObj(index -> Creation.of("t" + index, "payload")).toList());

        tester.awaitIdle();

        assertThat(runs).hasValue(50);
        assertThat(breaker.openTopics()).isEmpty();
    }

    /**
     * The probe that closes the breaker wakes the topic: the poll that saw the probe go out waits
     * for the next one, and would otherwise sleep that long before it claims again.
     */
    @Test
    void a_closing_probe_lets_the_topic_claim_at_once() throws Exception {
        AtomicInteger succeeded = new AtomicInteger();
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .extension(breaker)
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                if (down.get()) {
                    return Decision.failed("downstream refused");
                }
                succeeded.incrementAndGet();
                return Decision.success();
            }), topic -> topic.batchSize(5).concurrency(2).retry(Retry.none()).pollInterval(Duration.ofMinutes(5))
                .set(CircuitBreaker.WINDOW, 5).set(CircuitBreaker.MIN_TASKS, 2).set(CircuitBreaker.PROBE_EVERY, Duration.ofSeconds(20))))
            .build()
            .start();
        engine.tasks().independently().push(WORK, Insertion.APPEND,
            IntStream.range(0, 5).mapToObj(index -> Creation.of("t" + index, "payload")).toList());
        await("the breaker to open", Duration.ofSeconds(10), () -> breaker.isOpen(WORK));
        down.set(false);
        clock.advance(Duration.ofSeconds(21));

        // One push, one wake-up: the probe takes one task; the other waits for the probe to close the breaker.
        engine.tasks().independently().push(WORK, Insertion.APPEND, List.of(Creation.of("a", "payload"), Creation.of("b", "payload")));

        await("both tasks to succeed, well before the next probe or poll", Duration.ofSeconds(5), () -> succeeded.get() == 2);
        assertThat(breaker.isOpen(WORK)).isFalse();
    }

    @Test
    void an_override_turns_it_off() {
        try (TaskEngine built = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .extension(breaker)
            .overrides(Map.of("topics.work.breaker.enabled", "off", "topics.work.breaker.probe-every", "1m"))
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> Decision.success())))
            .build()) {
            assertThat(built.effectiveSettings()).singleElement().satisfies(settings -> {
                assertThat(settings.get("breaker.enabled")).isEqualTo(new EffectiveSettings.Setting("false", EffectiveSettings.Source.OVERRIDE));
                assertThat(settings.get("breaker.probe-every").value()).isEqualTo("PT1M");
                assertThat(settings.get("breaker.window").source()).isEqualTo(EffectiveSettings.Source.DEFAULT);
            });
        }
    }

    @Test
    void one_instance_keeps_each_engines_breakers_apart() {
        TaskTester tester = start(Retry.standard());
        try (TaskEngine other = TaskEngine.builder().store(new InMemoryTaskStore(clock)).extension(breaker)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> { })).build().start()) {
            engine.tasks().independently().push(WORK, Insertion.APPEND,
                IntStream.range(0, 20).mapToObj(index -> Creation.of("t" + index, "payload")).toList());
            tester.awaitIdle();

            assertThat(breaker.isOpen(WORK)).isTrue();
            assertThat(view(engine).rows()).hasSize(1);
            assertThat(view(other).rows()).as("the other engine's breaker is its own").isEmpty();
        }
    }

    @Test
    @DisplayName("Operators see an open breaker in its view and the health, and can close it")
    void operators_see_an_open_breaker_and_can_close_it() throws Exception {
        TaskTester tester = start(Retry.standard());
        engine.tasks().independently().push(WORK, Insertion.APPEND,
            IntStream.range(0, 20).mapToObj(index -> Creation.of("t" + index, "payload")).toList());
        tester.awaitIdle();

        OperatorView view = view(engine);
        assertThat(view.rows()).singleElement().satisfies(row -> {
            assertThat(row.key()).isEqualTo("work");
            assertThat(row.values().get("opened")).isNotNull();
            assertThat(row.values().get("failed")).asString().endsWith("of the last 10");
            assertThat(row.actions()).containsExactly("close");
        });
        assertThat(view.topic("work")).isEqualTo(OperatorView.TopicState.of("breaker open", "close"));
        await("the health to show the held topic", Duration.ofSeconds(5), () -> !engine.heldTopics().isEmpty());
        assertThat(engine.heldTopics()).singleElement().satisfies(held -> {
            assertThat(held.gate()).isEqualTo("CircuitBreaker");
            assertThat(held.reason()).isEqualTo("breaker open");
        });

        down.set(false);
        view.actions().getFirst().perform().perform("work");
        assertThat(breaker.isOpen(WORK)).isFalse();
        assertThat(view.topic("work")).isNull();
        assertThat(tester.drain(WORK).total()).isZero();
        assertThat(breaker.close(WORK)).as("closed already").isFalse();
    }

    @Test
    @DisplayName("While open, only the probe's own result closes it, not one of a batch that ran before it opened")
    void only_the_probes_own_result_closes_it() {
        TopicBreaker topic = new TopicBreaker(0.5, 2, 1, Duration.ofSeconds(30), clock);
        assertThat(topic.record(List.of(failed(1), failed(2)))).isEqualTo(TopicBreaker.Change.OPENED);

        clock.advance(Duration.ofSeconds(31));
        assertThat(topic.permit().probing()).isTrue();
        topic.claimed(List.of(7L));
        assertThat(topic.record(List.of(new BatchListener.Completed(3, "t3", 1, TaskObserver.Result.SUCCEEDED))))
            .as("a batch that was running when it opened").isEqualTo(TopicBreaker.Change.NONE);
        assertThat(topic.isOpen()).isTrue();
        assertThat(topic.record(List.of(new BatchListener.Completed(7, "t7", 1, TaskObserver.Result.SUCCEEDED))))
            .isEqualTo(TopicBreaker.Change.CLOSED);
    }

    private static BatchListener.Completed failed(long id) {
        return new BatchListener.Completed(id, "t" + id, 1, TaskObserver.Result.RETRYING);
    }

    private static OperatorView view(TaskEngine engine) {
        return engine.operatorViews().stream().filter(view -> view.name().equals("breaker")).findFirst().orElseThrow();
    }

    @Test
    void settings_parse_their_overrides_and_refuse_bad_values() {
        assertThat(CircuitBreaker.ENABLED.parse("off")).isFalse();
        assertThat(CircuitBreaker.ENABLED.parse(" TRUE ")).isTrue();
        assertThatThrownBy(() -> CircuitBreaker.ENABLED.parse("yes")).isInstanceOf(IllegalArgumentException.class);
        assertThat(CircuitBreaker.OPENS_AT.parse("0.5")).isEqualTo(0.5);
        assertThatThrownBy(() -> CircuitBreaker.OPENS_AT.parse("0")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CircuitBreaker.OPENS_AT.parse("1.5")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CircuitBreaker.OPENS_AT.parse("NaN")).isInstanceOf(IllegalArgumentException.class);
        assertThat(CircuitBreaker.WINDOW.parse("20")).isEqualTo(20);
        assertThatThrownBy(() -> CircuitBreaker.MIN_TASKS.parse("0")).isInstanceOf(IllegalArgumentException.class);
        assertThat(CircuitBreaker.PROBE_EVERY.parse("45s")).isEqualTo(Duration.ofSeconds(45));
        assertThat(CircuitBreaker.PROBE_EVERY.parse("PT2M")).isEqualTo(Duration.ofMinutes(2));
        assertThatThrownBy(() -> CircuitBreaker.PROBE_EVERY.parse("0s")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TaskEngine.builder().store(new InMemoryTaskStore(clock)).extension(breaker)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> { }, topic -> topic.set(CircuitBreaker.OPENS_AT, 2.0))).build())
            .as("set in code, checked like an override").hasMessageContaining("breaker.opens-at: must be above 0 and at most 1");
    }

    @FunctionalInterface
    private interface Work<V> {
        V run(StoreTransaction transaction) throws Exception;
    }

    private static <V> V transact(TaskStore store, Work<V> work) throws Exception {
        try (StoreTransaction transaction = store.begin()) {
            V value = work.run(transaction);
            transaction.commit();
            return value;
        }
    }

    private static void await(String what, Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting for " + what);
            }
            Thread.sleep(5);
        }
    }
}
