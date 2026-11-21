package io.github.khopland.pgtasks.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import org.junit.jupiter.api.Test;

/** A node that only pushes: prepared, never started; other nodes process what it pushes. */
class ProducerOnlyTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    @Test
    void a_prepared_engine_pushes_and_processes_nothing() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        List<String> handled = new CopyOnWriteArrayList<>();
        TaskConfigurer work = tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
            handled.add(task.payload());
            return Decision.success();
        }));
        try (TaskEngine producer = TaskEngine.builder().store(store).configure("work", work).build().prepare()) {
            // It registered the topic, so it can push before any processing node has started.
            long id = producer.tasks().independently().push(WORK, "a", "from the producer").id();
            Thread.sleep(200);
            assertThat(producer.running()).isFalse();
            assertThat(producer.processing()).isEmpty();
            assertThat(producer.health().running()).isFalse();
            assertThat(handled).isEmpty();
            assertThat(producer.limits().list()).isEmpty();

            try (TaskEngine consumer = TaskEngine.builder().store(store).configure("work", work).build().start()) {
                TaskTester.of(consumer).awaitIdle();
                assertThat(handled).containsExactly("from the producer");
                assertThat(consumer.admin().find(id).orElseThrow().state()).isEqualTo("succeeded");
            }
        }
    }
}
