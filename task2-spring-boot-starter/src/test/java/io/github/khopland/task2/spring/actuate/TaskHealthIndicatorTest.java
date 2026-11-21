package io.github.khopland.task2.spring.actuate;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.khopland.task2.Decision;
import io.github.khopland.task2.TopicRef;
import io.github.khopland.task2.engine.TaskEngine;
import io.github.khopland.task2.memory.InMemoryTaskStore;
import io.github.khopland.task2.spring.TaskHealthIndicator;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

class TaskHealthIndicatorTest {

    @Test
    void out_of_service_until_started_then_up_with_details() throws Exception {
        try (TaskEngine engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .node("pod-1")
            .configure("work", tasks -> tasks.topic(TopicRef.of("work", String.class), batch -> batch.decide(task -> Decision.success())))
            .build()) {
            TaskHealthIndicator indicator = new TaskHealthIndicator(engine);
            assertThat(indicator.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);

            engine.start();
            Health health = indicator.health();
            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails()).containsEntry("node", "pod-1").containsEntry("stuckClaims", 0).containsKey("heartbeatAge");

            engine.stop(java.time.Duration.ofSeconds(5));
            assertThat(indicator.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
        }
    }
}
