package org.example.task2.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.example.task2.Creation;
import org.example.task2.Decision;
import org.example.task2.Decisions;
import org.example.task2.Insertion;
import org.example.task2.Juncture;
import org.example.task2.TopicRef;
import org.example.task2.memory.InMemoryTaskStore;
import org.example.task2.spi.TaskStore.TaskView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Handlers that fail in unusual ways: an Error instead of an exception, or decisions for some tasks only. */
class FailureHandlingTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);
    private static final TopicRef<String> PARTS = TopicRef.of("parts", String.class);
    private static final TopicRef<String> MERGE = TopicRef.of("merge", String.class);

    private TaskEngine engine;

    @AfterEach
    void stop() {
        if (engine != null) {
            engine.close();
        }
    }

    private TaskView deadLetterAfter(org.example.task2.BatchHandler<String> handler) throws Exception {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .configure("work", tasks -> tasks.topic(WORK, handler, topic -> topic.retry(Retry.none())))
            .build()
            .start();
        long id = engine.tasks().independently().push(WORK, "a", "payload").id();
        TaskTester.of(engine).awaitIdle();
        return engine.find(id).orElseThrow();
    }

    /** Review 2026-11-21: an undecodable payload was retried as a counted failure for a week, about 343 times. */
    @Test
    void a_payload_the_codec_cant_decode_is_dead_lettered_at_once() throws Exception {
        TopicRef<Integer> numbers = TopicRef.of("numbers", Integer.class);
        List<Integer> handled = new CopyOnWriteArrayList<>();
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .configure("numbers", tasks -> tasks
                // Writes what it can't read for negative numbers, like a codec changed in a later version.
                .codec(Integer.class, org.example.task2.PayloadCodec.<Integer>of(n -> n < 0 ? "minus one" : String.valueOf(n), Integer::valueOf))
                .topic(numbers, batch -> batch.decide(task -> {
                    handled.add(task.payload());
                    return Decision.success();
                }), topic -> topic.retry(Retry.standard())))
            .build()
            .start();
        long good = engine.tasks().independently().push(numbers, "a", 1).id();
        long bad = engine.tasks().independently().push(numbers, "b", -1).id();
        TaskTester.of(engine).awaitIdle();

        assertThat(handled).containsExactly(1);
        assertThat(engine.find(good).orElseThrow().finished()).isTrue();
        TaskView view = engine.find(bad).orElseThrow();
        assertThat(view.state()).as("no retries: the stored text won't change").isEqualTo("failed");
        assertThat(view.attempt()).isEqualTo(1);
        assertThat(view.error()).contains("Could not decode the payload").contains("NumberFormatException");
    }

    /** Review F2: an Error escaped, and the orphan sweep handed the claim back uncounted, forever. */
    @Test
    @DisplayName("A handler that throws an Error fails its batch like an exception, counted and recorded")
    void an_error_from_the_handler_is_a_counted_failure() throws Exception {
        TaskView view = deadLetterAfter(batch -> {
            throw new NoClassDefFoundError("com/example/Missing");
        });

        assertThat(view.state()).as("dead letter under Retry.none()").isEqualTo("failed");
        assertThat(view.finished()).isFalse();
        assertThat(view.attempt()).isEqualTo(1);
        assertThat(view.error()).contains("NoClassDefFoundError").contains("com/example/Missing");
    }

    @Test
    @DisplayName("An Error from one task's decider fails that task")
    void an_error_from_a_decider_fails_the_task() throws Exception {
        TaskView view = deadLetterAfter(batch -> batch.decide(task -> {
            throw new ExceptionInInitializerError("static setup failed");
        }));

        assertThat(view.state()).isEqualTo("failed");
        assertThat(view.error()).contains("ExceptionInInitializerError");
    }

    /** Review F5: past its deadline a batch was retried while its handler, ignoring the interrupt, still ran. */
    @Test
    @DisplayName("A batch past its deadline keeps its tasks claimed until its handler has really ended")
    void a_handler_ignoring_the_deadline_never_runs_next_to_its_retry() throws Exception {
        org.example.task2.spi.ManualClock clock = org.example.task2.spi.ManualClock.startingNow();
        java.util.concurrent.atomic.AtomicInteger runs = new java.util.concurrent.atomic.AtomicInteger(), running = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger mostAtOnce = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicBoolean finish = new java.util.concurrent.atomic.AtomicBoolean();
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .globals(globals -> globals.heartbeat(java.time.Duration.ofMillis(50), java.time.Duration.ofHours(1))
                .housekeeping(housekeeping -> housekeeping.promoteEvery(java.time.Duration.ofMillis(20))))
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                runs.incrementAndGet();
                mostAtOnce.accumulateAndGet(running.incrementAndGet(), Math::max);
                try {
                    while (!finish.get()) {
                        java.util.concurrent.locks.LockSupport.parkNanos(1_000_000); // returns on interrupt, and carries on
                    }
                } finally {
                    running.decrementAndGet();
                }
                return Decision.success();
            }), topic -> topic.batchDeadline(java.time.Duration.ofMillis(200))))
            .build()
            .start();
        long id = engine.tasks().independently().push(WORK, "a", "payload").id();
        EngineFixture.await("the handler to start", java.time.Duration.ofSeconds(5), () -> runs.get() == 1);

        Thread.sleep(1_500); // past the deadline and the second the scope waits
        clock.advance(java.time.Duration.ofHours(1)); // far past any retry backoff
        Thread.sleep(300);

        assertThat(engine.find(id).orElseThrow().state()).as("still claimed").isEqualTo("active");
        assertThat(engine.health().stuck()).extracting(EngineHealth.StuckClaim::taskId).containsExactly(id);
        assertThat(runs).hasValue(1);

        finish.set(true);
        EngineFixture.await("the late batch to fail", java.time.Duration.ofSeconds(5),
            () -> !"active".equals(unchecked(() -> engine.find(id).orElseThrow().state())));
        TaskView view = engine.find(id).orElseThrow();
        assertThat(view.state()).isEqualTo("retrying");
        assertThat(view.error()).contains("deadline");
        assertThat(mostAtOnce).hasValue(1);
    }

    private static <V> V unchecked(java.util.concurrent.Callable<V> call) {
        try {
            return call.call();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Review F3: covering() rebuilt the decisions without the junctions. */
    @Test
    @DisplayName("Junctions staged by decided tasks survive when the handler leaves other tasks out")
    void junctions_survive_a_partial_set_of_decisions() throws Exception {
        List<String> merged = new CopyOnWriteArrayList<>();
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .configure("flow", tasks -> tasks
                .topic(PARTS, batch -> {
                    Decisions all = batch.decide(task -> {
                        if (task.identifier().equals("a")) {
                            task.junction(MERGE, Juncture.SINGULAR, Set.of("b"));
                        }
                        return Decision.success();
                    });
                    // Decide only task a; x is left out and fails.
                    Decisions.Builder some = Decisions.builder();
                    all.verdicts().stream().filter(verdict -> verdict.task().identifier().equals("a"))
                        .forEach(verdict -> some.decide(verdict.task(), verdict.decision(), verdict.downstream(), verdict.junctions()));
                    return some.build();
                }, topic -> topic.batchSize(10).retry(Retry.none()))
                .topic(MERGE, batch -> batch.decide(task -> {
                    merged.add(task.identifier());
                    return Decision.success();
                })))
            .build()
            .start();
        engine.tasks().independently().push(MERGE, Insertion.APPEND, List.of(Creation.of("a", "a").asSuspended(), Creation.of("b", "b").asSuspended()));
        engine.pause("parts");
        engine.tasks().independently().push(PARTS, Insertion.APPEND, List.of(Creation.of("a", "a"), Creation.of("x", "x")));
        engine.resume("parts");
        TaskTester.of(engine).awaitIdle();

        assertThat(merged).containsExactlyInAnyOrder("a", "b");
    }
}
