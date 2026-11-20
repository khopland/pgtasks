package org.example.task2.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.example.task2.Decision;
import org.example.task2.TopicRef;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class EngineSmokeTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final TopicRef<String> GREET = TopicRef.of("greet", String.class);

    private EngineFixture fixture;

    @BeforeEach
    void setUp(PostgresDatabase database) {
        fixture = new EngineFixture(database);
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    @Test
    void processes_a_pushed_task() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        TaskEngine engine = fixture.start("node-a", List.of(Topic.of(GREET, batch -> batch.decide(task -> {
            seen.add(task.identifier() + ":" + task.payload());
            return Decision.success();
        }))));

        engine.tasks().independently().push(GREET, "person-1", "hello");

        fixture.awaitQueueEmpty(Duration.ofSeconds(10));
        assertThat(seen).containsExactly("person-1:hello");
        assertThat(fixture.database.rows("SELECT state, attempt FROM tasks.task_view")).containsExactly(List.of("succeeded", 1));
    }
}
