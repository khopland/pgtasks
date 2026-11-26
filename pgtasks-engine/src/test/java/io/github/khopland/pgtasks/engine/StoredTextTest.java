package io.github.khopland.pgtasks.engine;

import java.util.List;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.TaskView;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Review 2026-11-25: a task's output, filter reason and failure message were stored whole. An
 * exception carrying a whole response body, as HTTP clients' do, stored megabytes per failure, on
 * every retry, written in the completion of the whole batch.
 */
class StoredTextTest {

    private static final TopicRef<String> TOPIC = TopicRef.of("topic", String.class);

    private final MemoryEngines engines = new MemoryEngines();

    @AfterEach
    void tearDown() {
        engines.close();
    }

    @Test
    void long_outputs_reasons_and_messages_are_cut() throws Exception {
        String huge = "x".repeat(1_000_000);
        TaskEngine engine = engines.start("node-a", List.of(Topic.of(TOPIC, batch -> batch.decide(task -> switch (task.payload()) {
            case "output" -> Decision.success(huge);
            case "filter" -> Decision.filtered(huge);
            default -> Decision.failed(new IllegalStateException("response body: " + huge));
        })).with(TopicSettings.defaults().withRetry(Retry.none()))));
        TaskTester tester = engines.tester();
        long output = engine.tasks().independently().push(TOPIC, "a", "output").id();
        long filter = engine.tasks().independently().push(TOPIC, "b", "filter").id();
        long failure = engine.tasks().independently().push(TOPIC, "c", "fail").id();
        tester.drain(TOPIC);

        TaskView succeeded = tester.find(output).orElseThrow();
        assertThat(succeeded.output()).hasSizeLessThan(BatchRun.MAX_OUTPUT + 100)
            .startsWith("xxx").endsWith("... (" + (1_000_000 - BatchRun.MAX_OUTPUT) + " more characters)");
        assertThat(tester.find(filter).orElseThrow().output()).hasSizeLessThan(BatchRun.MAX_OUTPUT + 100);
        TaskView failed = tester.find(failure).orElseThrow();
        assertThat(failed.state()).isEqualTo("failed");
        assertThat(failed.error()).hasSizeLessThan(ErrorJson.MAX_MESSAGE + 5_000).contains("response body: xxx").contains("more characters)");
    }

    @Test
    void a_cut_never_splits_a_character() {
        String text = "a".repeat(9) + "😀" + "b";
        assertThat(ErrorJson.cut(text, 10)).isEqualTo("a".repeat(9) + "... (3 more characters)");
        assertThat(ErrorJson.cut(text, 11)).isEqualTo("a".repeat(9) + "😀... (1 more characters)");
        assertThat(ErrorJson.cut(text, 12)).isEqualTo(text);
    }
}
