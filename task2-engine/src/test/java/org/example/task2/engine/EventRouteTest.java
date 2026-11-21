package org.example.task2.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.example.task2.Creation;
import org.example.task2.Decision;
import org.example.task2.Insertion;
import org.example.task2.Push;
import org.example.task2.Pushed;
import org.example.task2.TopicRef;
import org.example.task2.memory.InMemoryTaskStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Event routes, and topics created on push for module tests, without Spring. */
class EventRouteTest {

    private static final TopicRef<String> NOTIFY = TopicRef.of("notify", String.class);
    private static final TopicRef<String> ELSEWHERE = TopicRef.of("elsewhere", String.class);

    sealed interface Event permits Paid, Refunded {
    }

    record Paid(String id) implements Event {
    }

    record Refunded(String id) implements Event {
    }

    private TaskEngine engine;

    @AfterEach
    void stop() {
        if (engine != null) {
            engine.close();
        }
    }

    @Test
    @DisplayName("Routes take their event type and its subtypes, and may push nothing")
    void routes_push_for_matching_events() throws Exception {
        List<String> handled = new CopyOnWriteArrayList<>();
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .configure("notify-module", tasks -> tasks
                .onEvent(Paid.class, paid -> new Push<>(NOTIFY, Insertion.APPEND, Creation.of(paid.id(), "paid " + paid.id())))
                .onEvent(Event.class, event -> event instanceof Refunded refunded
                    ? new Push<>(NOTIFY, Insertion.APPEND, Creation.of(refunded.id(), "refunded " + refunded.id())) : null)
                .topic(NOTIFY, batch -> batch.decide(task -> {
                    handled.add(task.payload());
                    return Decision.success();
                })))
            .build()
            .start();

        assertThat(engine.eventRoutes()).extracting(EventRoute::module).containsOnly("notify-module");
        assertThat(engine.publish(new Paid("1"))).extracting(Pushed::status).containsExactly(Pushed.Status.QUEUED);
        assertThat(engine.publish(new Refunded("2"))).hasSize(1);
        assertThat(engine.publish("not an event")).isEmpty();
        TaskTester.of(engine).awaitIdle();

        assertThat(handled).containsExactlyInAnyOrder("paid 1", "refunded 2");
    }

    @Test
    @DisplayName("Only with topicsCreatedOnPush does a push to a topic nobody registered create it")
    void topics_are_created_on_push_only_when_asked() throws Exception {
        engine = TaskEngine.builder().store(new InMemoryTaskStore())
            .configure("notify-module", tasks -> tasks.topic(NOTIFY, batch -> batch.decide(task -> Decision.success())))
            .build().start();
        assertThatThrownBy(() -> engine.tasks().independently().push(ELSEWHERE, "a", "x")).hasMessageContaining("Unknown topic 'elsewhere'");
        engine.close();

        engine = TaskEngine.builder().store(new InMemoryTaskStore()).topicsCreatedOnPush(true)
            .configure("notify-module", tasks -> tasks.topic(NOTIFY, batch -> batch.decide(task -> Decision.success())))
            .build().start();
        TaskTester tester = TaskTester.of(engine);
        engine.tasks().independently().push(ELSEWHERE, "a", "x");
        tester.awaitIdle();

        assertThat(tester.pushed(ELSEWHERE)).extracting(Creation::payload).containsExactly("x");
        assertThat(tester.counts(ELSEWHERE).ready()).as("nobody runs it").isEqualTo(1);
        assertThat(engine.processing()).containsExactly("notify");
    }
}
