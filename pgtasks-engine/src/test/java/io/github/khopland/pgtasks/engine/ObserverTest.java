package io.github.khopland.pgtasks.engine;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** What a {@link TaskObserver} is told, and that a failing one doesn't hurt the work. */
class ObserverTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);
    private static final String TRACE = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    /** Records everything, and stores {@link #TRACE} with every push. */
    static final class Recording implements TaskObserver {
        final List<String> events = new CopyOnWriteArrayList<>();
        final Map<Result, Integer> results = new ConcurrentHashMap<>();
        final List<String> traces = new CopyOnWriteArrayList<>();

        @Override
        public PushObservation pushing(TopicRef<?> topic, int tasks) {
            events.add("pushing " + topic.name() + " " + tasks);
            return new PushObservation() {
                @Override
                public String traceContext() {
                    return TRACE;
                }

                @Override
                public void close() {
                    events.add("pushed-written " + topic.name());
                }
            };
        }

        @Override
        public void pushed(TopicRef<?> topic, List<Pushed> pushed) {
            events.add("pushed " + topic.name() + " " + pushed.size());
        }

        @Override
        public BatchObservation batch(TopicRef<?> topic, List<Task<?>> tasks) {
            tasks.forEach(task -> traces.add(task.traceContext()));
            return new BatchObservation() {
                @Override
                public void error(Throwable error) {
                    events.add("error " + error.getMessage());
                }

                @Override
                public void completed(Map<Result, Integer> counts) {
                    counts.forEach((result, count) -> results.merge(result, count, Integer::sum));
                }

                @Override
                public void close() {
                    events.add("batch-closed " + topic.name());
                }
            };
        }
    }

    private TaskEngine engine;

    @AfterEach
    void stop() {
        if (engine != null) {
            engine.close();
        }
    }

    private TaskEngine start(TaskObserver observer) throws Exception {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .observer(observer)
            .configure("work", tasks -> tasks.topic(WORK, batch -> {
                if (batch.payloads().contains("throw")) {
                    throw new IllegalStateException("the handler threw");
                }
                return batch.decide(task -> switch (task.payload()) {
                    case "fail" -> Decision.failed("no");
                    case "filter" -> Decision.filtered("not needed");
                    default -> Decision.success();
                });
            }, topic -> topic.retry(Retry.none()).batchSize(1)))
            .build()
            .start();
        return engine;
    }

    @Test
    @DisplayName("Pushes carry the observer's trace, and each batch's results are reported once committed")
    void pushes_carry_the_trace_and_results_are_reported() throws Exception {
        Recording observer = new Recording();
        start(observer);
        engine.tasks().independently().push(WORK, "a", "ok");
        engine.tasks().independently().push(WORK, "b", "fail");
        engine.tasks().independently().push(WORK, "c", "filter");
        engine.tasks().independently().push(WORK, "d", "throw");
        TaskTester.of(engine).awaitIdle();

        assertThat(observer.traces).hasSize(4).containsOnly(TRACE);
        assertThat(observer.results).containsExactlyInAnyOrderEntriesOf(Map.of(
            TaskObserver.Result.SUCCEEDED, 1, TaskObserver.Result.FILTERED, 1, TaskObserver.Result.DEAD_LETTERED, 2));
        assertThat(observer.events).contains("pushing work 1", "pushed-written work", "pushed work 1", "error the handler threw");
        assertThat(observer.events).filteredOn(event -> event.startsWith("batch-closed")).hasSize(4);
    }

    @Test
    @DisplayName("A push is observed while it is written, and reported once it has committed")
    void a_push_is_observed_while_written_then_reported() throws Exception {
        Recording observer = new Recording();
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .observer(observer)
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> Decision.success())))
            .build()
            .start();
        engine.admin().pause("work");
        engine.tasks().independently().push(WORK, "a", "ok");
        assertThat(observer.events).containsExactly("pushing work 1", "pushed-written work", "pushed work 1");
    }

    @Test
    @DisplayName("An observer that throws is logged, and the work goes on")
    void a_failing_observer_does_not_stop_the_work() throws Exception {
        List<String> handled = new CopyOnWriteArrayList<>();
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .observer(new TaskObserver() {
                @Override
                public PushObservation pushing(TopicRef<?> topic, int tasks) {
                    throw new IllegalStateException("broken observer");
                }

                @Override
                public BatchObservation batch(TopicRef<?> topic, List<Task<?>> tasks) {
                    return new BatchObservation() {
                        @Override
                        public Scope open() {
                            throw new IllegalStateException("broken observer");
                        }

                        @Override
                        public void completed(Map<Result, Integer> results) {
                            throw new IllegalStateException("broken observer");
                        }
                    };
                }
            })
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                handled.add(task.payload());
                return Decision.success();
            })))
            .build()
            .start();
        engine.tasks().independently().push(WORK, "a", "ok");
        TaskTester.of(engine).awaitIdle();

        assertThat(handled).containsExactly("ok");
        assertThat(TaskTester.of(engine).counts(WORK).total()).isZero();
    }

    @Test
    void several_observers_each_see_everything_even_when_one_throws() {
        List<String> seen = new CopyOnWriteArrayList<>();
        TaskObserver throwing = new TaskObserver() {
            @Override
            public PushObservation pushing(TopicRef<?> topic, int tasks) {
                throw new IllegalStateException("a broken observer");
            }

            @Override
            public void pushed(TopicRef<?> topic, List<Pushed> results) {
                throw new IllegalStateException("a broken observer");
            }
        };
        TaskObserver tracing = new TaskObserver() {
            @Override
            public PushObservation pushing(TopicRef<?> topic, int tasks) {
                return new PushObservation() {
                    @Override
                    public String traceContext() {
                        return TRACE;
                    }

                    @Override
                    public void close() {
                    }
                };
            }

            @Override
            public void pushed(TopicRef<?> topic, List<Pushed> results) {
                seen.add(topic.name());
            }
        };
        List<String> traces = new CopyOnWriteArrayList<>();
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore())
            .observer(throwing).observer(tracing)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> traces.add(task.task().traceContext())))
            .build().start()) {
            engine.tasks().independently().push(WORK, "a", "payload");
            TaskTester.of(engine).awaitIdle();

            assertThat(seen).containsExactly("work");
            assertThat(traces).as("the trace the second observer gave was stored").containsExactly(TRACE);
        }
    }
}
