package io.github.khopland.pgtasks.engine;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import io.github.khopland.pgtasks.Batch;
import io.github.khopland.pgtasks.BatchHandler;
import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Decisions;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Push;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Headers on tasks, and each task of a batch opened on its own for its decider and its downstream work. */
class HeadersTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);
    private static final TopicRef<String> NEXT = TopicRef.of("next", String.class);
    private static final String KEY = "korrelasjonsid";

    private final List<Map<String, String>> headers = new CopyOnWriteArrayList<>();
    private final List<String> events = new CopyOnWriteArrayList<>();
    private TaskEngine engine;

    @AfterEach
    void stop() {
        MDC.clear();
        if (engine != null) {
            engine.close();
        }
    }

    /** Gives every push a header, and records which task was open when each push started. */
    static final class Recording implements TaskObserver {

        private final List<String> events;
        private final ThreadLocal<String> current = new ThreadLocal<>();

        Recording(List<String> events) {
            this.events = events;
        }

        @Override
        public PushObservation pushing(TopicRef<?> topic, int tasks) {
            String from = current.get();
            events.add("push " + topic.name() + " x" + tasks + " from " + from);
            return new PushObservation() {
                @Override
                public Map<String, String> headers() {
                    return Map.of("pushed-from", String.valueOf(from), KEY, "from the observer");
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public BatchObservation batch(TopicRef<?> topic, List<Task<?>> tasks) {
            return new BatchObservation() {
                @Override
                public TaskObservation task(Task<?> task) {
                    events.add("task " + task.identifier());
                    return new TaskObservation() {
                        @Override
                        public Scope open() {
                            String previous = current.get();
                            current.set(task.identifier());
                            return () -> current.set(previous);
                        }

                        @Override
                        public void error(Throwable error) {
                            events.add("error " + task.identifier() + ": " + error.getMessage());
                        }

                        @Override
                        public void close() {
                            events.add("closed " + task.identifier());
                        }
                    };
                }

                @Override
                public void close() {
                    events.add("batch closed " + topic.name());
                }
            };
        }
    }

    private TaskEngine engine(TaskObserver observer, BatchHandler<String> work) {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .observer(observer)
            .configure("work", tasks -> tasks
                .topic(WORK, work, topic -> topic.batchSize(10))
                .eachTask(NEXT, task -> headers.add(task.task().headers())))
            .build()
            .prepare();
        return engine;
    }

    private void run() {
        engine.start();
        TaskTester.of(engine).awaitIdle();
    }

    @Test
    @DisplayName("A creation's headers are stored with the observers', and win over them")
    void a_creations_headers_are_stored_and_win() {
        engine(new Recording(events), batch -> Decisions.all(batch.tasks(), Decision.success()));
        engine.tasks().independently().push(NEXT, Insertion.APPEND, Creation.of("a", "x").withHeader(KEY, "mine").withHeaders(Map.of("tenant", "t1")));
        run();

        assertThat(headers).containsExactly(Map.of("pushed-from", "null", KEY, "mine", "tenant", "t1"));
    }

    @Test
    @DisplayName("Each task is opened around its decider and again for its downstream work, then closed once")
    void each_task_is_opened_for_its_decider_and_its_downstream_work() {
        engine(new Recording(events), batch -> batch.decide(task -> {
            task.push(NEXT, task.identifier() + "-1", "x");
            task.push(NEXT, task.identifier() + "-2", "x");
            return task.identifier().equals("c") ? Decision.failed("c fails") : Decision.success();
        }));
        for (String identifier : List.of("a", "b", "c")) {
            engine.tasks().independently().push(WORK, identifier, "x");
        }
        events.clear();
        run();

        assertThat(events).filteredOn(event -> event.startsWith("push next"))
            .as("one push per parent, started with the parent open; c failed, so its work isn't pushed")
            .containsExactly("push next x2 from a", "push next x2 from b");
        assertThat(headers).extracting(each -> each.get("pushed-from")).containsExactlyInAnyOrder("a", "a", "b", "b");
        assertThat(events).filteredOn(event -> event.startsWith("closed") && !event.contains("-")).containsExactly("closed a", "closed b", "closed c");
        assertThat(events.indexOf("batch closed work")).isGreaterThan(events.indexOf("push next x2 from b"));
    }

    @Test
    @DisplayName("A decider that throws is reported on its task, and decideConcurrently opens each task on its own thread")
    void a_throwing_decider_is_reported_on_its_task() {
        engine(new Recording(events), batch -> batch.decideConcurrently(3, task -> {
            if (task.identifier().equals("b")) {
                throw new IllegalStateException("b threw");
            }
            task.push(NEXT, task.identifier() + "-1", "x");
            return Decision.success();
        }));
        for (String identifier : List.of("a", "b", "c")) {
            engine.tasks().independently().push(WORK, identifier, "x");
        }
        run();

        assertThat(events).contains("error b: b threw");
        assertThat(headers).extracting(each -> each.get("pushed-from")).containsExactlyInAnyOrder("a", "c");
    }

    @Test
    @DisplayName("A batch handler that decides on its own still pushes each task's downstream work from that task")
    void a_plain_batch_handler_pushes_downstream_work_from_each_task() {
        engine(new Recording(events), (Batch<String> batch) -> {
            Decisions.Builder decisions = Decisions.builder();
            batch.tasks().forEach(task -> decisions.decide(task, Decision.success(),
                List.of(new Push<>(NEXT, Insertion.APPEND, Creation.of(task.identifier() + "-1", "x")))));
            return decisions.build();
        });
        engine.tasks().independently().push(WORK, "a", "x");
        engine.tasks().independently().push(WORK, "b", "x");
        events.clear();
        run();

        assertThat(events).containsSubsequence("task a", "push next x1 from a", "task b", "push next x1 from b", "closed a", "closed b", "batch closed work");
    }

    @Test
    @DisplayName("MDC keys follow the work: stored at the push, back in the MDC for the handler, carried downstream")
    void mdc_keys_follow_the_work() {
        List<String> inHandler = new CopyOnWriteArrayList<>();
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .observer(TaskObserver.mdc(List.of(KEY)))
            .configure("work", tasks -> tasks
                .topic(WORK, batch -> batch.decide(task -> {
                    inHandler.add(task.identifier() + "=" + MDC.get(KEY));
                    task.push(NEXT, task.identifier(), "x");
                    return Decision.success();
                }), topic -> topic.batchSize(10))
                .eachTask(NEXT, task -> {
                    inHandler.add("next " + task.identifier() + "=" + MDC.get(KEY));
                    headers.add(task.task().headers());
                }))
            .build()
            .prepare();
        MDC.put(KEY, "k-a");
        engine.tasks().independently().push(WORK, "a", "x");
        MDC.put(KEY, "k-b");
        engine.tasks().independently().push(WORK, "b", "x");
        MDC.remove(KEY);
        engine.tasks().independently().push(WORK, "c", "x");
        run();

        assertThat(inHandler).containsExactlyInAnyOrder("a=k-a", "b=k-b", "c=null", "next a=k-a", "next b=k-b", "next c=null");
        assertThat(headers).containsExactlyInAnyOrder(Map.of(KEY, "k-a"), Map.of(KEY, "k-b"), Map.of());
    }

    @Test
    @DisplayName("Several observers' headers are all stored; for a name two give, the first one's")
    void several_observers_headers_are_merged() {
        TaskObserver second = new TaskObserver() {
            @Override
            public PushObservation pushing(TopicRef<?> topic, int tasks) {
                return new PushObservation() {
                    @Override
                    public Map<String, String> headers() {
                        return Map.of(KEY, "from the second", "second", "yes");
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };
        engine(TaskObserver.composite(List.of(new Recording(events), second)), batch -> Decisions.all(batch.tasks(), Decision.success()));
        engine.tasks().independently().push(NEXT, "a", "x");
        run();

        assertThat(headers).containsExactly(Map.of("pushed-from", "null", KEY, "from the observer", "second", "yes"));
    }

    @Test
    void headers_must_have_names_and_values() {
        Creation<String> creation = Creation.of("a", "x");
        assertThatThrownBy(() -> creation.withHeader("", "x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> creation.withHeaders(Collections.singletonMap("name", null))).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> TaskObserver.mdc(List.of(""))).isInstanceOf(IllegalArgumentException.class);
        assertThat(creation.withHeader("a", "1").withHeader("a", "2").headers()).containsExactly(Map.entry("a", "2"));
    }
}
