package org.example.task2.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.example.task2.Creation;
import org.example.task2.Decision;
import org.example.task2.Insertion;
import org.example.task2.TopicRef;
import org.example.task2.memory.InMemoryTaskStore;
import org.junit.jupiter.api.Test;

class ConcurrentDecideTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);
    private static final TopicRef<String> NEXT = TopicRef.of("next", String.class);

    /** Review: the node-wide cap was held for the whole batch, so hung handlers in one topic starved the others. */
    @Test
    void hung_batches_in_one_topic_dont_hold_up_another() throws Exception {
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
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
            EngineFixture.await("two hung batches, as many as the node allows", java.time.Duration.ofSeconds(5), () -> hung.get() == 2);

            engine.tasks().independently().push(NEXT, "x", "other topic");
            EngineFixture.await("the other topic to run", java.time.Duration.ofSeconds(5), () -> done.contains("other topic"));
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
            engine.start();
            engine.pause("work");
            engine.tasks().independently().push(WORK, Insertion.APPEND, IntStream.range(0, 20)
                .mapToObj(index -> Creation.of("t" + index, index == 7 ? "bad" : "fine")).toList());
            long started = System.nanoTime();
            engine.resume("work");
            TaskTester.of(engine).awaitIdle();
            long millis = (System.nanoTime() - started) / 1_000_000;

            assertThat(mostAtOnce).hasValue(5);
            assertThat(millis).as("four rounds of 100 ms, not twenty").isLessThan(1_500);
            assertThat(next).hasSize(19).doesNotContain("after t7");
            assertThat(engine.find(8).orElseThrow().state()).as("only the bad one failed").isEqualTo("failed");
        }
    }
}
