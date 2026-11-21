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
