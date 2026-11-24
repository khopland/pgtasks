package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.Work;
import io.github.khopland.pgtasks.postgres.TaskSchema;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Engines on a database whose default isolation is REPEATABLE READ, as a pool's
 * transactionIsolation or a role's default_transaction_isolation can make it. The store runs its
 * own transactions at READ COMMITTED, so two nodes claiming and completing one topic at once
 * don't fail with serialization errors.
 */
class RepeatableReadTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> {
        // Before any pool connects, so every connection starts at the default.
        database.execute("ALTER DATABASE " + database.name() + " SET default_transaction_isolation = 'repeatable read'");
        TaskSchema.migrate(database.dataSource());
    });

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);
    private static final TopicRef<String> NEXT = TopicRef.of("next", String.class);
    private static final int TASKS = 200;

    private EngineFixture fixture;

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    @Test
    void two_nodes_process_one_topic_and_its_downstream_work(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        fixture.createTopics(WORK.name(), NEXT.name());
        List<String> done = new CopyOnWriteArrayList<>();
        TopicSettings small = TopicSettings.defaults().withBatchSize(5);
        Topic<String> work = Topic.of(WORK, batch -> batch.decide(task -> {
            task.push(NEXT, Insertion.APPEND, Creation.of(task.identifier(), task.payload()));
            return Decision.success();
        })).with(small);
        Topic<String> next = Topic.of(NEXT, batch -> batch.decide(task -> {
            done.add(task.identifier());
            return Decision.success();
        })).with(small);
        fixture.start("node-a", List.of(work, next));
        fixture.start("node-b", List.of(work, next));

        TaskEngine producer = fixture.engines().getFirst();
        for (int index = 0; index < TASKS; index++) {
            producer.tasks().independently().push(WORK, "t" + index, "x");
        }

        EngineFixture.await("every task and its downstream work", Duration.ofSeconds(30), () -> done.size() == TASKS);
        assertThat(done).doesNotHaveDuplicates();
        fixture.awaitQueueEmpty(Duration.ofSeconds(10));
    }
}
