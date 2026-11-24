package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import io.github.khopland.pgtasks.BatchHandler;
import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Decisions;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.PayloadCodec;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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

    private TaskView deadLetterAfter(BatchHandler<String> handler) throws Exception {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .configure("work", tasks -> tasks.topic(WORK, handler, topic -> topic.retry(Retry.none())))
            .build()
            .start();
        long id = engine.tasks().independently().push(WORK, "a", "payload").id();
        TaskTester.of(engine).awaitIdle();
        return engine.admin().find(id).orElseThrow();
    }

    /** Review 2026-11-21: one task pushing to a topic that doesn't exist failed the whole batch's completion. */
    @Test
    void a_push_to_a_topic_that_doesnt_exist_fails_only_the_task_that_staged_it() throws Exception {
        TopicRef<String> next = TopicRef.of("next", String.class);
        List<String> nexts = new CopyOnWriteArrayList<>();
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .configure("work", tasks -> tasks
                .topic(WORK, batch -> batch.decide(task -> {
                    task.push(task.payload().equals("typo") ? TopicRef.of("nxet", String.class) : next, task.identifier(), task.payload());
                    return Decision.success();
                }), topic -> topic.retry(Retry.none()))
                .topic(next, batch -> batch.decide(task -> {
                    nexts.add(task.payload());
                    return Decision.success();
                })))
            .build()
            .start();
        engine.tasks().independently().push(WORK, Insertion.APPEND, List.of(Creation.of("a", "fine"), Creation.of("b", "typo"), Creation.of("c", "fine too")));
        TaskTester.of(engine).awaitIdle();

        assertThat(nexts).containsExactlyInAnyOrder("fine", "fine too");
        TaskView typo = engine.admin().find(2).orElseThrow();
        assertThat(typo.state()).isEqualTo("failed");
        assertThat(typo.error()).contains("Unknown topic 'nxet'");
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
                .codec(Integer.class, PayloadCodec.<Integer>of(n -> n < 0 ? "minus one" : String.valueOf(n), Integer::valueOf))
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
        assertThat(engine.admin().find(good).orElseThrow().finished()).isTrue();
        TaskView view = engine.admin().find(bad).orElseThrow();
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
        ManualClock clock = ManualClock.startingNow();
        AtomicInteger runs = new AtomicInteger(), running = new AtomicInteger();
        AtomicInteger mostAtOnce = new AtomicInteger();
        AtomicBoolean finish = new AtomicBoolean();
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .globals(globals -> globals.heartbeat(Duration.ofMillis(50), Duration.ofHours(1))
                .housekeeping(housekeeping -> housekeeping.promoteEvery(Duration.ofMillis(20))))
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                runs.incrementAndGet();
                mostAtOnce.accumulateAndGet(running.incrementAndGet(), Math::max);
                try {
                    while (!finish.get()) {
                        LockSupport.parkNanos(1_000_000); // returns on interrupt, and carries on
                    }
                } finally {
                    running.decrementAndGet();
                }
                return Decision.success();
            }), topic -> topic.batchDeadline(Duration.ofMillis(200))))
            .build()
            .start();
        long id = engine.tasks().independently().push(WORK, "a", "payload").id();
        EngineFixture.await("the handler to start", Duration.ofSeconds(5), () -> runs.get() == 1);

        Thread.sleep(1_500); // past the deadline and the second the scope waits
        clock.advance(Duration.ofHours(1)); // far past any retry backoff
        Thread.sleep(300);

        assertThat(engine.admin().find(id).orElseThrow().state()).as("still claimed").isEqualTo("active");
        assertThat(engine.health().stuck()).extracting(EngineHealth.StuckClaim::taskId).containsExactly(id);
        assertThat(runs).hasValue(1);

        finish.set(true);
        EngineFixture.await("the late batch to fail", Duration.ofSeconds(5),
            () -> !"active".equals(unchecked(() -> engine.admin().find(id).orElseThrow().state())));
        TaskView view = engine.admin().find(id).orElseThrow();
        assertThat(view.state()).isEqualTo("retrying");
        assertThat(view.error()).contains("deadline");
        assertThat(mostAtOnce).hasValue(1);
    }

    private static <V> V unchecked(Callable<V> call) {
        try {
            return call.call();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
