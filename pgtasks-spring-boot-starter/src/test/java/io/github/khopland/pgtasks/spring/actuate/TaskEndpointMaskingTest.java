package io.github.khopland.pgtasks.spring.actuate;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.Retry;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spring.TaskEndpoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.endpoint.SecurityContext;
import org.springframework.boot.actuate.endpoint.Show;

/** Review 2026-11-21: the endpoint showed every payload and stack trace to anyone who could reach it. */
class TaskEndpointMaskingTest {

    static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    private final TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore())
        .configure("work", tasks -> tasks.eachTask(WORK, task -> {
            if (task.payload().startsWith("type")) {
                throw new IllegalArgumentException("person 12345678901 is unknown");
            }
            throw new IllegalStateException("never") {
                @Override
                public String getMessage() {
                    return "person 12345678901 is unknown";
                }
            };
        }, topic -> topic.retry(Retry.none().deadLetterAt(IllegalArgumentException.class))))
        .build().start();

    @AfterEach
    void stop() {
        engine.stop(Duration.ofSeconds(5));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> first(Object body, String list) {
        return ((List<Map<String, Object>>) ((Map<String, Object>) body).get(list)).getFirst();
    }

    @Test
    void payloads_and_messages_are_hidden_unless_shown() throws Exception {
        engine.tasks().independently().push(WORK, "a", "type: fnr 12345678901");
        TaskTester.of(engine).awaitIdle();

        TaskEndpoint hidden = new TaskEndpoint(engine);
        Map<String, Object> dead = first(hidden.readPart("topics", "work", "dead-letters", null, null, SecurityContext.NONE).getBody(), "deadLetters");
        assertThat(dead.get("payload")).isEqualTo("******");
        assertThat((String) dead.get("error")).contains("java.lang.IllegalArgumentException").doesNotContain("12345678901");
        Map<String, Object> group = first(hidden.readPart("topics", "work", "failing", null, null, SecurityContext.NONE).getBody(), "failing");
        assertThat(group.get("error")).as("a type is shown").isEqualTo("java.lang.IllegalArgumentException");
        assertThat(group.get("exampleMessage")).isEqualTo("******");
        @SuppressWarnings("unchecked")
        Map<String, Object> task = (Map<String, Object>) hidden.read("tasks", "1", SecurityContext.NONE).getBody();
        assertThat(task.get("payload")).isEqualTo("******");

        TaskEndpoint shown = new TaskEndpoint(engine, Show.ALWAYS, Set.of());
        Map<String, Object> visible = first(shown.readPart("topics", "work", "dead-letters", null, null, SecurityContext.NONE).getBody(), "deadLetters");
        assertThat(visible.get("payload")).isEqualTo("type: fnr 12345678901");
        assertThat((String) visible.get("error")).contains("person 12345678901 is unknown");
    }
}
