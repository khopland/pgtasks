package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Phase 3 exit test: a stop drains the node and leaves no pgtasks threads behind, and no claims.
 * Threads are counted from the JDK's thread dump, virtual ones included, against those alive
 * before the test.
 */
class ShutdownTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final TopicRef<String> SLOW = TopicRef.of("slow", String.class);

    private final MemoryEngines engines = new MemoryEngines();
    private Map<Long, String> before;
    private EngineFixture fixture;

    @BeforeEach
    void baseline() {
        before = ThreadDump.named("pgtasks-");
    }

    @AfterEach
    void tearDown() {
        engines.close();
        if (fixture != null) {
            fixture.close();
        }
    }

    private Map<Long, String> threadsLeft() {
        Map<Long, String> left = new TreeMap<>(ThreadDump.named("pgtasks-"));
        left.keySet().removeAll(before.keySet());
        return left;
    }

    private static List<Creation<String>> tasks(String prefix, int count) {
        return IntStream.range(0, count).mapToObj(index -> Creation.of(prefix + index, "x")).toList();
    }

    /** Every kind of thread the engine starts: pollers, batches, batch scopes, housekeeping, notifier, reloads, a drain. */
    private List<Topic<?>> busyTopics(AtomicInteger runs, CountDownLatch running) {
        return List.of(
            Topic.of(SLOW, batch -> {
                running.countDown();
                Thread.sleep(100);
                return batch.decide(task -> {
                    runs.incrementAndGet();
                    return Decision.success();
                });
            }).with(TopicSettings.defaults().withBatchSize(5).withConcurrency(2)));
    }

    @Test
    void a_stop_leaves_no_threads_and_no_claims_behind(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        AtomicInteger runs = new AtomicInteger();
        CountDownLatch running = new CountDownLatch(2);
        TaskEngine nodeA = fixture.create("node-a", busyTopics(runs, running), settings -> settings).start();
        TaskEngine nodeB = fixture.create("node-b", busyTopics(runs, running), settings -> settings).start();
        nodeA.tasks().independently().push(SLOW, Insertion.APPEND, tasks("s", 200));
        assertThat(running.await(5, TimeUnit.SECONDS)).as("batches running on both nodes").isTrue();
        assertThat(threadsLeft()).as("the engines' threads while running").isNotEmpty();

        long started = System.nanoTime();
        nodeA.stop(Duration.ofSeconds(5));
        nodeB.stop(Duration.ofSeconds(5));
        Duration stopping = Duration.ofNanos(System.nanoTime() - started);

        assertThat(threadsLeft()).as("pgtasks threads left after stop").isEmpty();
        assertThat(fixture.count("SELECT count(*) FROM pgtasks.queue WHERE state = 'active'")).as("claims left").isZero();
        assertThat(fixture.count("SELECT count(*) FROM pgtasks.lease")).as("the housekeeping lease is given up").isZero();
        assertThat(stopping).as("running batches finish in well under the timeout").isLessThan(Duration.ofSeconds(3));
        assertThat(runs.get()).isLessThan(200);
        assertThat(fixture.count("SELECT count(*) FROM pgtasks.queue q JOIN pgtasks.topic t ON t.id = q.topic_id WHERE t.name = 'slow' AND q.attempt > 0"))
            .as("unfinished tasks wait with no attempt counted").isZero();
    }

    @Test
    void a_batch_past_the_stop_deadline_is_cancelled_and_its_tasks_handed_back(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        CountDownLatch running = new CountDownLatch(1);
        TaskEngine engine = fixture.start("node-a", List.of(Topic.of(SLOW, batch -> {
            running.countDown();
            Thread.sleep(Duration.ofHours(1));
            return batch.decide(task -> Decision.success());
        })));
        engine.tasks().independently().push(SLOW, Insertion.APPEND, tasks("s", 3));
        assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();

        long started = System.nanoTime();
        engine.stop(Duration.ofMillis(300));
        Duration stopping = Duration.ofNanos(System.nanoTime() - started);

        assertThat(threadsLeft()).as("pgtasks threads left after stop").isEmpty();
        assertThat(stopping).isLessThan(Duration.ofSeconds(2));
        // Handed back as retrying and due now; a promotion that was already running can make them
        // ready before the stop returns, which is the same thing a moment later.
        assertThat(database.rows("SELECT CASE WHEN state = 'retrying' AND run_at <= now() THEN 'ready' ELSE state::text END, attempt "
            + "FROM pgtasks.queue ORDER BY task_id"))
            .as("handed back, due at once and keeping its place in line, the cancelled attempt not counted")
            .containsExactly(List.of("ready", 0), List.of("ready", 0), List.of("ready", 0));
    }

    @Test
    void a_stop_on_the_in_memory_store_leaves_no_threads_behind() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        CountDownLatch running = new CountDownLatch(1);
        TaskEngine engine = engines.start("node-a", busyTopics(runs, running));
        engine.tasks().independently().push(SLOW, Insertion.APPEND, tasks("s", 50));
        assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();

        engine.stop(Duration.ofSeconds(5));

        assertThat(threadsLeft()).as("pgtasks threads left after stop").isEmpty();
    }
}
