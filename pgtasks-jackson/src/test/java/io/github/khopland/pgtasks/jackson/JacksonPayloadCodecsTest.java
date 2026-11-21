package io.github.khopland.pgtasks.jackson;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class JacksonPayloadCodecsTest {

    record Order(String id, int quantity, List<String> items) {
    }

    static final TopicRef<Order> ORDERS = TopicRef.of("orders", Order.class);

    @Test
    void a_record_payload_round_trips_as_json_without_a_codec_of_its_own() {
        List<Order> handled = new CopyOnWriteArrayList<>();
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore())
            .codecs(new JacksonPayloadCodecs(JsonMapper.builder().build()))
            .configure("orders", tasks -> tasks.eachTask(ORDERS, task -> handled.add(task.payload())))
            .build().start()) {
            Order order = new Order("o-1", 2, List.of("book", "pen"));
            long id = engine.tasks().independently().push(ORDERS, order.id(), order).id();
            TaskTester.of(engine).awaitIdle();

            assertThat(handled).containsExactly(order);
            TaskView view = engine.admin().find(id).orElseThrow();
            assertThat(view.payloadType()).isEqualTo("json");
            assertThat(view.payload()).isEqualTo("{\"id\":\"o-1\",\"quantity\":2,\"items\":[\"book\",\"pen\"]}");
        }
    }
}
