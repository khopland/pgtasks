package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.ArrayList;
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

/**
 * Review 2026-11-25: when the completion of a batch failed for a reason the engine doesn't retry,
 * every task in the batch was recorded as failed, the ones whose own work was fine included. One
 * task whose downstream push the database refused took the nine beside it down: all ten ran three
 * times and all ten were dead-lettered.
 */
class CompletionIsolationTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);
    private static final TopicRef<String> ITEMS = TopicRef.of("items", String.class);

    private EngineFixture fixture;

    @AfterEach
    void stop() {
        if (fixture != null) {
            fixture.close();
        }
    }

    @Test
    void a_downstream_push_the_database_refuses_fails_only_the_task_that_staged_it(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        fixture.createTopics(WORK.name(), ITEMS.name());
        // Anything the database refuses for one push alone and the engine can't foresee: a constraint,
        // a trigger, a value too long for an index.
        database.execute("""
            CREATE FUNCTION pgtasks.refuse_poison() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.identifier = 'poison' THEN
                    RAISE EXCEPTION 'refused: %', NEW.identifier;
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER refuse_poison BEFORE INSERT ON pgtasks.task FOR EACH ROW EXECUTE FUNCTION pgtasks.refuse_poison();
            """);
        Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        Topic<String> work = Topic.of(WORK, batch -> batch.decide(task -> {
            runs.computeIfAbsent(task.identifier(), ignored -> new AtomicInteger()).incrementAndGet();
            task.push(ITEMS, task.payload(), "x");
            return Decision.success();
        }));
        work = work.with(work.settings().withRetry(Retry.exponential(Duration.ofMillis(20), Duration.ofMillis(20)).attempts(3)).withBatchSize(10));
        TaskEngine engine = fixture.create("node-a", List.of(work), settings -> settings);
        List<Creation<String>> creations = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            creations.add(Creation.of("fine-" + i, "item-" + i));
        }
        creations.add(Creation.of("bad", "poison"));
        List<Long> ids = engine.tasks().independently().push(WORK, Insertion.APPEND, creations).stream().map(pushed -> pushed.id()).toList();
        engine.start();

        EngineFixture.await("the batch settled", Duration.ofSeconds(20),
            () -> database.count("SELECT count(*) FROM pgtasks.queue q JOIN pgtasks.topic t ON t.id = q.topic_id WHERE t.name = 'work' AND q.state <> 'failed'") == 0);

        for (long id : ids.subList(0, 9)) {
            TaskView view = engine.admin().find(id).orElseThrow();
            assertThat(view.state()).as(view.identifier()).isEqualTo("succeeded");
            assertThat(runs.get(view.identifier())).as(view.identifier() + " ran once").hasValue(1);
        }
        TaskView bad = engine.admin().find(ids.get(9)).orElseThrow();
        assertThat(bad.state()).as("dead-lettered once its attempts are used up").isEqualTo("failed");
        assertThat(bad.attempt()).isEqualTo(3);
        assertThat(bad.error()).contains("refused: poison");
        assertThat(database.count("SELECT count(*) FROM pgtasks.task t JOIN pgtasks.topic tp ON tp.id = t.topic_id WHERE tp.name = 'items'"))
            .as("the nine good tasks' downstream work, once each").isEqualTo(9);
    }
}
