package io.github.khopland.pgtasks.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Review 2026-11-21: a per-task handler, topics a module pushes to, and pushes from a handler that should be staged. */
class HandlerShapesTest {

    record Order(String id) {
    }

    private static final TopicRef<String> NOTIFY = TopicRef.of("notify", String.class);
    private static final TopicRef<String> ARCHIVE = TopicRef.of("archive", String.class);
    private static final TopicRef<Order> ORDERS = TopicRef.of("orders", Order.class);

    private final List<TaskEngine> engines = new CopyOnWriteArrayList<>();

    @AfterEach
    void stop() {
        engines.forEach(TaskEngine::close);
    }

    private TaskEngine started(TaskEngineBuilder builder) {
        TaskEngine engine = builder.build().start();
        engines.add(engine);
        return engine;
    }

    @Test
    void a_per_task_handler_succeeds_by_returning_and_fails_by_throwing() {
        List<String> archived = new CopyOnWriteArrayList<>();
        TaskEngine engine = started(TaskEngine.builder().store(new InMemoryTaskStore()).configure("notify", tasks -> tasks
            .eachTask(NOTIFY, task -> {
                if (task.payload().equals("bad")) {
                    throw new IllegalArgumentException("can't send " + task.payload());
                }
                task.push(ARCHIVE, task.identifier(), "sent " + task.payload());
            }, topic -> topic.retry(Retry.none()))
            .eachTask(ARCHIVE, task -> archived.add(task.payload()))));
        engine.tasks().independently().push(NOTIFY, Insertion.APPEND, List.of(Creation.of("a", "hello"), Creation.of("b", "bad")));
        TaskTester.of(engine).awaitIdle();

        assertThat(archived).containsExactly("sent hello");
        TaskView bad = engine.admin().find(2).orElseThrow();
        assertThat(bad.state()).isEqualTo("failed");
        assertThat(bad.error()).contains("can't send bad");
    }

    @Test
    void a_topic_a_module_pushes_to_exists_before_the_module_that_runs_it_starts() {
        InMemoryTaskStore store = new InMemoryTaskStore();
        TaskEngine producer = TaskEngine.builder().store(store)
            .configure("shop", tasks -> tasks.codec(Order.class, io.github.khopland.pgtasks.PayloadCodec.of(Order::id, Order::new)).pushesTo(ORDERS))
            .build().prepare();
        engines.add(producer);
        producer.tasks().independently().push(ORDERS, "o-1", new Order("o-1"));

        List<Order> handled = new CopyOnWriteArrayList<>();
        TaskEngine consumer = started(TaskEngine.builder().store(store)
            .configure("billing", tasks -> tasks.codec(Order.class, io.github.khopland.pgtasks.PayloadCodec.of(Order::id, Order::new))
                .eachTask(ORDERS, task -> handled.add(task.payload()))));
        TaskTester.of(consumer).awaitIdle();
        assertThat(handled).containsExactly(new Order("o-1"));
    }

    @Test
    void a_topic_pushed_to_without_a_codec_or_with_another_type_fails_the_build() {
        assertThatThrownBy(() -> TaskEngine.builder().store(new InMemoryTaskStore())
            .configure("shop", tasks -> tasks.pushesTo(ORDERS)).build())
            .hasMessageContaining("Topic orders, which shop pushes to: No payload codec for " + Order.class.getName());

        assertThatThrownBy(() -> TaskEngine.builder().store(new InMemoryTaskStore())
            .configure("billing", tasks -> tasks.eachTask(NOTIFY, task -> { }))
            .configure("shop", tasks -> tasks.pushesTo(TopicRef.of("notify", Integer.class))).build())
            .hasMessageContaining("Topic notify has payload type java.lang.String in billing, but shop pushes java.lang.Integer");
    }

    @Test
    void tasks_push_from_a_handler_without_a_transaction_says_to_stage_it() {
        List<Tasks> tasksOfEngine = new CopyOnWriteArrayList<>();
        TaskEngine engine = started(TaskEngine.builder().store(new InMemoryTaskStore()).configure("notify", tasks -> tasks
            .eachTask(NOTIFY, task -> tasksOfEngine.getFirst().push(ARCHIVE, task.identifier(), "should have been staged"),
                topic -> topic.retry(Retry.none()))
            .eachTask(ARCHIVE, task -> { })));
        tasksOfEngine.add(engine.tasks());
        engine.tasks().independently().push(NOTIFY, "a", "hello");
        TaskTester.of(engine).awaitIdle();

        assertThat(engine.admin().find(1).orElseThrow().error())
            .contains("from a handler of notify: there is no transaction to join. Stage the push with task.push(...)");
    }

    @Test
    void a_success_can_keep_an_output_on_the_task() {
        TaskEngine engine = started(TaskEngine.builder().store(new InMemoryTaskStore()).configure("notify", tasks -> tasks
            .topic(NOTIFY, batch -> batch.decide(task -> io.github.khopland.pgtasks.Decision.success("receipt-" + task.identifier())))));
        long id = engine.tasks().independently().push(NOTIFY, "a", "hello").id();
        TaskTester.of(engine).awaitIdle();

        assertThat(engine.admin().find(id).orElseThrow().output()).isEqualTo("receipt-a");
    }
}
