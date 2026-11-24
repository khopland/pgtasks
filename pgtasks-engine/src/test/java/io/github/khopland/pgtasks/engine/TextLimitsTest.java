package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Review 2026-11-24: Postgres text and jsonb can't hold a NUL character. A failure message,
 * output or filter reason with one made the whole batch's completion fail, so every task of the
 * batch, the ones that succeeded included, was recorded as failed and ran again.
 */
class TextLimitsTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    private EngineFixture fixture;

    @AfterEach
    void stop() {
        if (fixture != null) {
            fixture.close();
        }
    }

    @Test
    void a_nul_in_what_a_task_reports_affects_only_that_task(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        fixture.createTopics(WORK.name());
        Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        Topic<String> topic = Topic.of(WORK, batch -> batch.decide(task -> {
            runs.computeIfAbsent(task.identifier(), ignored -> new AtomicInteger()).incrementAndGet();
            return switch (task.payload()) {
                case "fail" -> Decision.failed(new IllegalStateException("bad byte \u0000 in input"));
                case "output" -> Decision.success("out\u0000put");
                case "filter" -> Decision.filtered("re\u0000ason");
                case "push" -> {
                    task.push(WORK, "next\u0000", "never");
                    yield Decision.success();
                }
                case "hostile" -> Decision.failed(new Hostile());
                default -> Decision.success();
            };
        }));
        topic = topic.with(topic.settings().withRetry(Retry.none()).withBatchSize(10));
        TaskEngine engine = fixture.create("node-a", List.of(topic), settings -> settings);
        List<Long> ids = engine.tasks().independently().push(WORK, Insertion.APPEND, List.of(
            Creation.of("fine", "fine"), Creation.of("fail", "fail"), Creation.of("output", "output"), Creation.of("filter", "filter"),
            Creation.of("push", "push"), Creation.of("hostile", "hostile")))
            .stream().map(pushed -> pushed.id()).toList();
        engine.start();

        EngineFixture.await("the dead letter", Duration.ofSeconds(10),
            () -> database.count("SELECT count(*) FROM pgtasks.queue WHERE state <> 'failed'") == 0
                && database.count("SELECT count(*) FROM pgtasks.queue") == 3);

        assertThat(runs.get("fine")).as("ran once: its completion wasn't lost to another task's text").hasValue(1);
        assertThat(engine.admin().find(ids.get(0)).orElseThrow().state()).isEqualTo("succeeded");
        TaskView failed = engine.admin().find(ids.get(1)).orElseThrow();
        assertThat(failed.state()).isEqualTo("failed");
        assertThat(failed.error()).contains("bad byte").contains("in input").contains("IllegalStateException");
        TaskView output = engine.admin().find(ids.get(2)).orElseThrow();
        assertThat(output.state()).isEqualTo("succeeded");
        assertThat(output.output()).startsWith("out").endsWith("put");
        assertThat(engine.admin().find(ids.get(3)).orElseThrow().state()).isEqualTo("filtered");
        TaskView push = engine.admin().find(ids.get(4)).orElseThrow();
        assertThat(push.state()).as("the bad push fails the task that staged it").isEqualTo("failed");
        assertThat(push.error()).contains("NUL");
        TaskView hostile = engine.admin().find(ids.get(5)).orElseThrow();
        assertThat(hostile.state()).as("recorded and dead-lettered, not handed back uncounted").isEqualTo("failed");
        assertThat(hostile.error()).contains(Hostile.class.getName());
    }

    @Test
    void a_nul_is_refused_where_a_creation_is_made() {
        assertThatThrownBy(() -> Creation.of("a\u0000b", "payload")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("identifier");
        assertThatThrownBy(() -> Creation.of("a", "payload").withHeader("name", "v\u0000")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Creation.of("a", "payload").withDedupKey("\u0000")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Creation.of("a", "payload").withReference("\u0000")).isInstanceOf(IllegalArgumentException.class);
    }

    /** An exception whose every description throws. */
    private static final class Hostile extends RuntimeException {
        @Override
        public String getMessage() {
            throw new UnsupportedOperationException("no message");
        }

        @Override
        public String toString() {
            throw new UnsupportedOperationException("no text");
        }
    }
}
