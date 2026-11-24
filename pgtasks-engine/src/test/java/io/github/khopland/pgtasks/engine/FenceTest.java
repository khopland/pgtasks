package io.github.khopland.pgtasks.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Review 2026-11-24: a node whose heartbeat couldn't get through, while the node otherwise
 * worked, kept running its handlers after other nodes took its tasks over, so one task ran on two
 * nodes at once. It now fences itself first: it cancels its handlers and stops claiming.
 */
class FenceTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);
    /** Fences after 1.6 s without a heartbeat; other nodes requeue its tasks after 2 s. */
    private static final UnaryOperator<EngineSettings> SHORT_EXPIRY = settings -> settings.withHeartbeat(Duration.ofMillis(100), Duration.ofSeconds(2));

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
    void a_node_whose_heartbeat_is_late_cancels_its_handlers_before_another_node_takes_its_tasks() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean runningOnA = new AtomicBoolean(), overlapped = new AtomicBoolean();
        List<String> ranOn = new CopyOnWriteArrayList<>();
        TaskEngine a = fixture.start("node-a", List.of(Topic.of(WORK, batch -> batch.decide(task -> {
            ranOn.add("a");
            runningOnA.set(true);
            started.countDown();
            try {
                Thread.sleep(Duration.ofMinutes(1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                runningOnA.set(false);
            }
            return Decision.success();
        }))), SHORT_EXPIRY);
        a.tasks().independently().push(WORK, "x", "payload");
        assertThat(started.await(5, TimeUnit.SECONDS)).as("node a runs the task").isTrue();
        fixture.start("node-b", List.of(Topic.of(WORK, batch -> batch.decide(task -> {
            ranOn.add("b");
            overlapped.compareAndSet(false, runningOnA.get());
            return Decision.success();
        }))), SHORT_EXPIRY);

        // Holds node a's heartbeat back and nothing else: a claim's foreign key check only shares the row.
        try (Connection lock = fixture.database.connect()) {
            lock.setAutoCommit(false);
            try (PreparedStatement statement = lock.prepareStatement("SELECT 1 FROM pgtasks.node WHERE id = ? FOR NO KEY UPDATE")) {
                statement.setInt(1, a.nodeId());
                statement.executeQuery().close();
            }
            EngineFixture.await("node a to cancel its handler", Duration.ofSeconds(5), () -> !runningOnA.get());
            EngineFixture.await("node b to run the task", Duration.ofSeconds(10), () -> ranOn.contains("b"));
            fixture.awaitQueueEmpty(Duration.ofSeconds(5));
            lock.rollback();
        }

        assertThat(overlapped).as("the task never ran on both nodes at once").isFalse();
        assertThat(ranOn).containsExactly("a", "b");
        assertThat(fixture.count("SELECT attempts FROM pgtasks.task")).as("node a handed it back without counting its attempt").isEqualTo(1);
    }

    @Test
    void a_fenced_node_claims_again_once_a_heartbeat_gets_through() throws Exception {
        List<String> ran = new CopyOnWriteArrayList<>();
        TaskEngine a = fixture.start("node-a", List.of(Topic.of(WORK, batch -> batch.decide(task -> {
            ran.add(task.identifier());
            return Decision.success();
        }))), SHORT_EXPIRY);

        try (Connection lock = fixture.database.connect()) {
            lock.setAutoCommit(false);
            try (PreparedStatement statement = lock.prepareStatement("SELECT 1 FROM pgtasks.node WHERE id = ? FOR NO KEY UPDATE")) {
                statement.setInt(1, a.nodeId());
                statement.executeQuery().close();
            }
            Thread.sleep(2_000);
            a.tasks().independently().push(WORK, "while-fenced", "payload");
            Thread.sleep(500);
            assertThat(ran).as("no claims while fenced").isEmpty();
            lock.rollback();
        }

        EngineFixture.await("node a to claim again", Duration.ofSeconds(5), () -> ran.contains("while-fenced"));
    }
}
