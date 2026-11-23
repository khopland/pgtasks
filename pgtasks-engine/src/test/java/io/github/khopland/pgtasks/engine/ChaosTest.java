package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.Work;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.github.khopland.pgtasks.engine.EngineFixture.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chaos on real Postgres: nodes die mid-batch, freeze until they are presumed dead, or lose the
 * database. Every task must still complete exactly once: one committed child per parent, and no
 * task left behind.
 */
class ChaosTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);
    private static final TopicRef<String> CHILD = TopicRef.of("child", String.class);

    private PostgresDatabase database;
    private final List<TaskEngine> engines = new ArrayList<>();
    private final List<ChaosDataSource> pools = new ArrayList<>();
    private final Map<Long, AtomicInteger> executions = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp(PostgresDatabase database) throws Exception {
        this.database = database;
        new EngineFixture(database).createTopics(WORK.name(), CHILD.name());
    }

    @AfterEach
    void tearDown() {
        pools.forEach(ChaosDataSource::restore);
        engines.forEach(engine -> engine.stop(Duration.ofSeconds(5)));
        pools.forEach(ChaosDataSource::close);
    }

    private ChaosDataSource pool(String node) {
        ChaosDataSource pool = new ChaosDataSource(database, node);
        pools.add(pool);
        return pool;
    }

    private TaskEngine node(String node, ChaosDataSource pool, Duration expiry, Consumer<Work<String>> during) throws Exception {
        EngineSettings settings = EngineFixture.settings(node)
            .withHeartbeat(Duration.ofMillis(200), expiry)
            .withPromoteInterval(Duration.ofMillis(200))
            .withNotifier(null);
        TaskEngine engine = new TaskEngine(new PostgresTaskStore(pool), settings, List.of(
            Topic.of(WORK, batch -> batch.decide(task -> {
                executions.computeIfAbsent(task.id(), ignored -> new AtomicInteger()).incrementAndGet();
                during.accept(task);
                task.push(CHILD, task.identifier(), "from " + node);
                return Decision.success();
            })).with(TopicSettings.defaults().withBatchSize(10).withConcurrency(2).withPollInterval(Duration.ofMillis(200)))
        ));
        engines.add(engine);
        return engine.start();
    }

    private void push(TaskEngine engine, int count) {
        engine.tasks().independently().push(WORK, Insertion.APPEND,
            IntStream.range(0, count).mapToObj(index -> Creation.of("t" + index, "x")).toList());
    }

    private long succeeded() {
        return database.count("SELECT count(*) FROM pgtasks.task_view WHERE topic = 'work' AND state = 'succeeded'");
    }

    /** No task lost, none completed twice: every parent succeeded and has exactly one committed child. */
    private void assertExactlyOnce(int count) {
        assertThat(succeeded()).as("succeeded tasks").isEqualTo(count);
        assertThat(database.count("SELECT count(*) FROM pgtasks.queue WHERE topic_id = (SELECT id FROM pgtasks.topic WHERE name = 'work')"))
            .as("unfinished tasks").isZero();
        assertThat(database.count("SELECT count(DISTINCT parent_id) FROM pgtasks.task_view WHERE topic = 'child'")).as("parents with a child").isEqualTo(count);
        assertThat(database.count("SELECT count(*) FROM pgtasks.task_view WHERE topic = 'child'")).as("children").isEqualTo(count);
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void killing_a_node_mid_batch_loses_nothing_and_completes_nothing_twice() throws Exception {
        ChaosDataSource doomed = pool("node-a");
        AtomicInteger startedByA = new AtomicInteger();
        CountDownLatch midBatch = new CountDownLatch(1);
        node("node-a", doomed, Duration.ofSeconds(2), task -> {
            if (startedByA.incrementAndGet() == 25) {
                midBatch.countDown();
            }
            sleep(Duration.ofMillis(20));
        });
        TaskEngine survivor = node("node-b", pool("node-b"), Duration.ofSeconds(2), task -> sleep(Duration.ofMillis(5)));
        push(survivor, 200);

        assertThat(midBatch.await(20, TimeUnit.SECONDS)).isTrue();
        doomed.cut(); // the process is gone: no more writes, its open transactions roll back

        await("every task to succeed", Duration.ofSeconds(60), () -> succeeded() == 200);
        assertExactlyOnce(200);
        assertThat(executions.values()).as("tasks the dead node was running ran again").anyMatch(count -> count.get() > 1);
        assertThat(database.count("SELECT count(*) FROM pgtasks.task WHERE error->>'type' = 'expired'"))
            .as("tasks the dead node was running, expired").isPositive();
    }

    @Test
    void a_node_frozen_until_it_is_presumed_dead_cannot_complete_its_batch() throws Exception {
        ChaosDataSource frozen = pool("node-a");
        AtomicBoolean pauseFirst = new AtomicBoolean(true);
        CountDownLatch paused = new CountDownLatch(1), resume = new CountDownLatch(1);
        TaskEngine nodeA = node("node-a", frozen, Duration.ofSeconds(2), task -> {
            if (pauseFirst.getAndSet(false)) {
                frozen.freeze();
                paused.countDown();
                try {
                    resume.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        await("node A to lead the housekeeping", Duration.ofSeconds(10), nodeA::leadsHousekeeping);
        push(nodeA, 50);
        assertThat(paused.await(20, TimeUnit.SECONDS)).isTrue();
        TaskEngine nodeB = node("node-b", pool("node-b"), Duration.ofSeconds(2), task -> { });

        // The frozen leader can't renew its lease, so node B takes over and requeues A's batch.
        await("node B to finish everything, including the frozen node's batch", Duration.ofSeconds(60), () -> succeeded() == 50);
        assertThat(nodeB.leadsHousekeeping()).as("node B took over the housekeeping").isTrue();
        frozen.restore();
        resume.countDown();
        sleep(Duration.ofSeconds(1)); // node A wakes up and tries to complete its stale batch

        assertExactlyOnce(50);
        assertThat(database.count("SELECT count(*) FROM pgtasks.task_view WHERE topic = 'child' AND payload = 'from node-a' AND parent_id IN "
            + "(SELECT t.id FROM pgtasks.task t WHERE t.error->>'type' = 'expired')")).as("children from the stale batch").isZero();
    }

    @Test
    void cutting_the_database_while_polling_backs_off_and_recovers() throws Exception {
        ChaosDataSource flaky = pool("node-a");
        AtomicInteger done = new AtomicInteger();
        TaskEngine node = node("node-a", flaky, Duration.ofSeconds(10), task -> {
            done.incrementAndGet();
            sleep(Duration.ofMillis(10));
        });
        push(node, 100);
        await("some progress", Duration.ofSeconds(20), () -> done.get() >= 30);

        flaky.cut();
        sleep(Duration.ofSeconds(2));
        int refused = flaky.refused();
        flaky.restore();

        await("every task to succeed", Duration.ofSeconds(60), () -> succeeded() == 100);
        assertExactlyOnce(100);
        // Polls, completions and housekeeping all back off: no hot loop while the database is gone.
        assertThat(refused).as("connection attempts during a 2 s outage").isBetween(1, 150);
    }

    @Test
    void killed_backends_mid_completion_are_retried_without_double_completion() throws Exception {
        ChaosDataSource unlucky = pool("node-a");
        TaskEngine node = node("node-a", unlucky, Duration.ofSeconds(10), task -> sleep(Duration.ofMillis(2)));
        push(node, 300);

        long until = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < until && succeeded() < 300) {
            unlucky.terminate();
            sleep(Duration.ofMillis(50));
        }

        await("every task to succeed", Duration.ofSeconds(60), () -> succeeded() == 300);
        assertExactlyOnce(300);
    }
}
