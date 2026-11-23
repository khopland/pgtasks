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
import io.github.khopland.pgtasks.Junction;
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
 * own transactions at READ COMMITTED, so claims don't fail with serialization errors and a fan-in
 * whose parts complete at the same time on two nodes still releases.
 */
class RepeatableReadTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> {
        // Before any pool connects, so every connection starts at the default.
        database.execute("ALTER DATABASE " + database.name() + " SET default_transaction_isolation = 'repeatable read'");
        TaskSchema.migrate(database.dataSource());
    });

    private static final TopicRef<String> LEFT = TopicRef.of("left", String.class);
    private static final TopicRef<String> RIGHT = TopicRef.of("right", String.class);
    private static final TopicRef<String> MERGE = TopicRef.of("merge", String.class);
    private static final int PAIRS = 20;

    private EngineFixture fixture;

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    @Test
    void parts_completing_at_once_on_two_nodes_release_every_merge(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        fixture.createTopics(LEFT.name(), RIGHT.name(), MERGE.name());
        // Each pair's two parts finish their handlers together, so both nodes complete them at once.
        Map<String, CyclicBarrier> barriers = new ConcurrentHashMap<>();
        List<String> merged = new CopyOnWriteArrayList<>();
        TopicSettings one = TopicSettings.defaults().withBatchSize(1);
        Topic<String> merge = Topic.of(MERGE, batch -> batch.decide(task -> {
            merged.add(task.identifier());
            return Decision.success();
        }));
        fixture.start("node-a", List.of(Topic.of(LEFT, batch -> batch.decide(task -> part(task, barriers))).with(one), merge));
        fixture.start("node-b", List.of(Topic.of(RIGHT, batch -> batch.decide(task -> part(task, barriers))).with(one), merge));

        TaskEngine producer = fixture.engines().getFirst();
        for (int pair = 0; pair < PAIRS; pair++) {
            producer.tasks().independently().push(LEFT, "left-" + pair, "right-" + pair);
            producer.tasks().independently().push(RIGHT, "right-" + pair, "left-" + pair);
        }

        EngineFixture.await("every merge released and processed", Duration.ofSeconds(30), () -> merged.size() == 2 * PAIRS);
        assertThat(merged).containsExactlyInAnyOrderElementsOf(IntStream.range(0, PAIRS)
            .boxed().flatMap(pair -> List.of("left-" + pair, "right-" + pair).stream()).toList());
        fixture.awaitQueueEmpty(Duration.ofSeconds(10));
    }

    private static Decision part(Work<String> task, Map<String, CyclicBarrier> barriers) {
        String pair = task.identifier().substring(task.identifier().indexOf('-') + 1);
        task.push(MERGE, Insertion.APPEND, Creation.of(task.identifier(), task.payload()).asSuspended());
        task.junction(MERGE, Junction.Counts.SUSPENDED, Set.of(task.payload()));
        try {
            barriers.computeIfAbsent(pair, ignored -> new CyclicBarrier(2)).await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("The pair's other part never arrived", e);
        }
        return Decision.success();
    }
}
