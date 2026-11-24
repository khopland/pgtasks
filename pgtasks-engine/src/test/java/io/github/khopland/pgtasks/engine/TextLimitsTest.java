package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Decisions;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.PayloadCodec;
import io.github.khopland.pgtasks.Push;
import io.github.khopland.pgtasks.Task;
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
 * Review 2026-11-24: Postgres text and jsonb can't hold a NUL character, nor a time past the year
 * 294276. A failure message,
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
                case "forever" -> Decision.later(Duration.ofSeconds(Long.MAX_VALUE));
                default -> Decision.success();
            };
        }));
        topic = topic.with(topic.settings().withRetry(Retry.none()).withBatchSize(10));
        TaskEngine engine = fixture.create("node-a", List.of(topic), settings -> settings);
        List<Long> ids = engine.tasks().independently().push(WORK, Insertion.APPEND, List.of(
            Creation.of("fine", "fine"), Creation.of("fail", "fail"), Creation.of("output", "output"), Creation.of("filter", "filter"),
            Creation.of("push", "push"), Creation.of("hostile", "hostile"), Creation.of("forever", "forever")))
            .stream().map(pushed -> pushed.id()).toList();
        engine.start();

        EngineFixture.await("the dead letter", Duration.ofSeconds(10),
            () -> database.count("SELECT count(*) FROM pgtasks.queue WHERE state <> 'failed'") == 0
                && database.count("SELECT count(*) FROM pgtasks.queue") == 4);

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
        TaskView forever = engine.admin().find(ids.get(6)).orElseThrow();
        assertThat(forever.state()).as("a delay the store can't hold fails only its task").isEqualTo("failed");
        assertThat(forever.error()).contains("at most a thousand years");
    }

    record Item(String text) {
    }

    private static final TopicRef<Item> ITEMS = TopicRef.of("items", Item.class);
    private static final TopicRef<Item> NOWHERE = TopicRef.of("nowhere", Item.class);

    /**
     * Review 2026-11-24: downstream payloads were encoded inside the completion, so one that the
     * codec couldn't encode, or that encoded to a NUL, or a push to a topic that doesn't exist from
     * a handler that builds its decisions itself, failed every task of the batch, on every retry.
     */
    @Test
    void downstream_work_that_cant_be_written_fails_only_the_task_that_staged_it(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        fixture.createTopics(WORK.name(), ITEMS.name());
        Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        PayloadCodec<Item> codec = PayloadCodec.of(item -> switch (item.text()) {
            case "boom" -> throw new IllegalStateException("can't encode " + item.text());
            case "nul" -> "a\u0000b";
            default -> item.text();
        }, Item::new);
        Topic<String> work = Topic.of(WORK, batch -> {
            Decisions.Builder decisions = Decisions.builder();
            for (Task<String> task : batch.tasks()) {
                runs.computeIfAbsent(task.identifier(), ignored -> new AtomicInteger()).incrementAndGet();
                TopicRef<Item> target = task.payload().equals("nowhere") ? NOWHERE : ITEMS;
                decisions.decide(task, Decision.success(), List.of(new Push<>(target, Insertion.APPEND, Creation.of(task.identifier(), new Item(task.payload())))));
            }
            return decisions.build();
        });
        work = work.with(work.settings().withRetry(Retry.none()).withBatchSize(10));
        Topic<Item> items = Topic.of(ITEMS, codec, batch -> batch.decide(task -> Decision.success()));
        TaskEngine engine = fixture.create("node-a", List.of(work, items), settings -> settings);
        List<Long> ids = engine.tasks().independently().push(WORK, Insertion.APPEND, List.of(
            Creation.of("fine", "fine"), Creation.of("boom", "boom"), Creation.of("nul", "nul"), Creation.of("nowhere", "nowhere")))
            .stream().map(pushed -> pushed.id()).toList();
        engine.start();

        EngineFixture.await("the dead letters", Duration.ofSeconds(10),
            () -> database.count("SELECT count(*) FROM pgtasks.queue WHERE state <> 'failed'") == 0
                && database.count("SELECT count(*) FROM pgtasks.queue") == 3);

        assertThat(runs.get("fine")).as("ran once: its completion wasn't lost to another task's push").hasValue(1);
        assertThat(engine.admin().find(ids.get(0)).orElseThrow().state()).isEqualTo("succeeded");
        assertThat(database.count("SELECT count(*) FROM pgtasks.task t JOIN pgtasks.topic p ON p.id = t.topic_id "
            + "WHERE p.name = 'items' AND t.identifier = 'fine'")).as("its downstream work was written").isEqualTo(1);
        assertThat(engine.admin().find(ids.get(1)).orElseThrow().error()).contains("can't encode boom");
        assertThat(engine.admin().find(ids.get(2)).orElseThrow().error()).contains("NUL");
        assertThat(engine.admin().find(ids.get(3)).orElseThrow().error()).contains("nowhere");
        List.of(1, 2, 3).forEach(index -> assertThat(engine.admin().find(ids.get(index)).orElseThrow().state()).isEqualTo("failed"));
    }

    /**
     * Review 2026-11-24: headers stored that a task can't be made with stopped work. Ones that
     * weren't a JSON object of strings (an edit by hand) failed the claim, which rolled back and
     * claimed the same task first again: the whole topic stopped, on every node. One with an empty
     * name (an observer's, before observers' headers were checked) failed the batch before it
     * ran, and its claims stayed held until the node restarted.
     */
    @Test
    void a_task_whose_stored_headers_cant_be_read_runs_without_them(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        fixture.createTopics(WORK.name());
        Map<String, Map<String, String>> seen = new ConcurrentHashMap<>();
        Topic<String> topic = Topic.of(WORK, batch -> batch.decide(task -> {
            seen.put(task.identifier(), task.task().headers());
            return Decision.success();
        }));
        TaskEngine engine = fixture.create("node-a", List.of(topic.with(topic.settings().withBatchSize(1))), settings -> settings);
        List<Long> ids = engine.tasks().independently().push(WORK, Insertion.APPEND, List.of(
            Creation.of("nested", "x"), Creation.of("unnamed", "x"), Creation.of("after", "x")))
            .stream().map(pushed -> pushed.id()).toList();
        database.execute("UPDATE pgtasks.task SET headers = '{\"a\": {\"b\": 1}}' WHERE id = " + ids.get(0));
        database.execute("UPDATE pgtasks.task SET headers = '{\"\": \"no name\", \"kept\": \"yes\"}' WHERE id = " + ids.get(1));
        engine.start();

        EngineFixture.await("every task to run", Duration.ofSeconds(10), () -> seen.size() == 3);
        assertThat(seen.get("nested")).isEmpty();
        assertThat(seen.get("unnamed")).containsExactly(Map.entry("kept", "yes"));
        ids.forEach(id -> assertThat(engine.admin().find(id).orElseThrow().state()).isEqualTo("succeeded"));
    }

    @Test
    void a_payload_that_encodes_to_a_nul_is_refused_when_it_is_pushed(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        fixture.createTopics(WORK.name());
        TaskEngine engine = fixture.create("node-a", List.of(Topic.of(WORK, batch -> batch.decide(task -> Decision.success()))), settings -> settings);
        assertThatThrownBy(() -> engine.tasks().independently().push(WORK, "a", "x\u0000y"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("NUL");
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
