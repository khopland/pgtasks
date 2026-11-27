package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConcurrentDecideTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);
    private static final TopicRef<String> NEXT = TopicRef.of("next", String.class);

    /** Review: the node-wide cap was held for the whole batch, so hung handlers in one topic starved the others. */
    @Test
    void hung_batches_in_one_topic_dont_hold_up_another() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger hung = new AtomicInteger();
        List<String> done = new CopyOnWriteArrayList<>();
        try (TaskEngine engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .globals(globals -> globals.maxInFlightBatches(2))
            .configure("work", tasks -> tasks
                .topic(WORK, batch -> {
                    hung.incrementAndGet();
                    release.await();
                    return batch.decide(task -> Decision.success());
                }, topic -> topic.batchSize(1).concurrency(2))
                .topic(NEXT, batch -> batch.decide(task -> {
                    done.add(task.payload());
                    return Decision.success();
                })))
            .build()
            .start()) {
            engine.tasks().independently().push(WORK, Insertion.APPEND, List.of(Creation.of("a", "a"), Creation.of("b", "b")));
            EngineFixture.await("two hung batches, as many as the node allows", Duration.ofSeconds(5), () -> hung.get() == 2);

            engine.tasks().independently().push(NEXT, "x", "other topic");
            EngineFixture.await("the other topic to run", Duration.ofSeconds(5), () -> done.contains("other topic"));
            release.countDown();
        } finally {
            release.countDown();
        }
    }

    @Test
    void up_to_n_tasks_are_decided_at_once_each_with_its_own_staged_work() throws Exception {
        AtomicInteger running = new AtomicInteger(), mostAtOnce = new AtomicInteger();
        List<String> next = new CopyOnWriteArrayList<>();
        try (TaskEngine engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .configure("work", tasks -> tasks
                .topic(WORK, batch -> batch.decideConcurrently(5, task -> {
                    mostAtOnce.accumulateAndGet(running.incrementAndGet(), Math::max);
                    try {
                        Thread.sleep(100); // a remote call
                        if (task.payload().equals("bad")) {
                            throw new IllegalStateException("bad input");
                        }
                        task.push(NEXT, task.identifier(), "after " + task.identifier());
                        return Decision.success();
                    } finally {
                        running.decrementAndGet();
                    }
                }), topic -> topic.batchSize(20).retry(Retry.none()))
                .topic(NEXT, batch -> batch.decide(task -> {
                    next.add(task.payload());
                    return Decision.success();
                })))
            .build()) {
            engine.prepare();
            engine.tasks().independently().push(WORK, Insertion.APPEND, IntStream.range(0, 20)
                .mapToObj(index -> Creation.of("t" + index, index == 7 ? "bad" : "fine")).toList());
            long started = System.nanoTime();
            engine.start();
            TaskTester.of(engine).awaitIdle();
            long millis = (System.nanoTime() - started) / 1_000_000;

            assertThat(mostAtOnce).hasValue(5);
            assertThat(millis).as("four rounds of 100 ms, not twenty").isLessThan(1_500);
            assertThat(next).hasSize(19).doesNotContain("after t7");
            assertThat(engine.admin().find(8).orElseThrow().state()).as("only the bad one failed").isEqualTo("dead_letter");
        }
    }

    /** Review 2026-11-21: past the deadline the task was retried while the cancelled decider still ran, five runs at once. */
    @Test
    void a_decider_that_ignores_cancellation_keeps_its_task_until_it_returns() throws Exception {
        AtomicInteger running = new AtomicInteger(), mostAtOnce = new AtomicInteger(), started = new AtomicInteger();
        try (TaskEngine engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .globals(globals -> globals.housekeeping(housekeeping -> housekeeping.promoteEvery(Duration.ofMillis(20))))
            .configure("work", tasks -> tasks
                .topic(WORK, batch -> batch.decideConcurrently(2, task -> {
                    started.incrementAndGet();
                    mostAtOnce.accumulateAndGet(running.incrementAndGet(), Math::max);
                    try {
                        long end = System.nanoTime() + 400_000_000L;
                        while (System.nanoTime() < end) {
                            Thread.onSpinWait(); // ignores interruption, like a call that can't be interrupted
                        }
                        return Decision.success();
                    } finally {
                        running.decrementAndGet();
                    }
                }), topic -> topic.batchSize(1).batchDeadline(Duration.ofMillis(50))
                    .retry(Retry.exponential(Duration.ofMillis(10), Duration.ofMillis(10)).attempts(10))))
            .build()
            .start()) {
            engine.tasks().independently().push(WORK, "one", "x");
            EngineFixture.await("a second run", Duration.ofSeconds(5), () -> started.get() >= 2);

            assertThat(mostAtOnce).as("never two runs of the task at once").hasValue(1);
        }
    }
}
