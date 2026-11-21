package io.github.khopland.task2.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.github.khopland.task2.Decision;
import io.github.khopland.task2.TopicRef;
import io.github.khopland.task2.memory.InMemoryTaskStore;
import org.junit.jupiter.api.Test;

/** Review 2026-11-21: stop() switched the reloads off for good, so an engine started again ignored notifications. */
class RestartTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);
    private static final TopicRef<String> OTHER = TopicRef.of("other", String.class);

    @Test
    void an_engine_started_again_after_a_stop_processes_and_hears_notifications() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        List<String> handled = new CopyOnWriteArrayList<>();
        // An hour between heartbeats: only a notification can carry the pause below.
        TaskGlobals slowHeartbeat = globals -> globals.heartbeat(Duration.ofHours(1), Duration.ofHours(2));
        TaskEngine engine = TaskEngine.builder().store(store).globals(slowHeartbeat)
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                handled.add(task.payload());
                return Decision.success();
            })))
            .build();
        try {
            engine.start();
            engine.stop(Duration.ofSeconds(5));
            engine.start();

            engine.tasks().independently().push(WORK, "a", "after the restart");
            EngineFixture.await("the task to run", Duration.ofSeconds(5), () -> handled.contains("after the restart"));

            try (TaskEngine other = TaskEngine.builder().store(store).globals(slowHeartbeat)
                .configure("other", tasks -> tasks.topic(OTHER, batch -> batch.decide(task -> Decision.success())))
                .build().start()) {
                other.admin().pause("work");
                EngineFixture.await("the pause to reach the restarted engine", Duration.ofSeconds(5),
                    () -> !engine.processing().contains("work"));
            }
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }
}
