package org.example.task2.slice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.example.task2.engine.EngineFixture.await;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.example.task2.Decision;
import org.example.task2.TopicRef;
import org.example.task2.engine.EngineFixture;
import org.example.task2.engine.Retry;
import org.example.task2.engine.TaskEngine;
import org.example.task2.engine.Topic;
import org.example.task2.engine.TopicSettings;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** The checklist from the infra guide's "Testing a task flow", applied to the slice. */
class SliceTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final Retry FAST = new Retry(5, Duration.ofMillis(50), 2, Duration.ofMillis(400), 0.5);

    private EngineFixture fixture;
    private PostgresDatabase database;

    @BeforeEach
    void setUp(PostgresDatabase database) {
        this.database = database;
        fixture = new EngineFixture(database);
        Slice.createDomainTables(database);
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    private List<Topic<?>> slice(AssessmentHandler assessments, FeedPublisher publisher) {
        return List.of(
            new Topic<>(Slice.ASSESSMENT, assessments, Slice.REQUESTS, TopicSettings.defaults().withRetry(FAST), List.of()),
            new Topic<>(Slice.NOTIFY, new NoticeHandler(publisher), Slice.NOTICES, TopicSettings.defaults().withRetry(FAST), List.of(publisher)),
            Topic.of(Slice.ARCHIVE, batch -> batch.decide(task -> Decision.success()))
        );
    }

    private void readFeed(TaskEngine engine) throws SQLException {
        try (Connection connection = database.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            new IncomeFeed(engine.tasks()).readPage(connection, 100);
            connection.commit();
        }
    }

    @Test
    @DisplayName("A normal flow succeeds end to end, with one lookup per batch and newer income superseding older")
    void normal_flow() throws Exception {
        PartsRegister register = new PartsRegister().register("anna", 30).register("bjorn", 40);
        TaskEngine engine = fixture.start("node-a", slice(new AssessmentHandler(register, new AssessmentLedger(database.dataSource())), new FeedPublisher()));
        database.execute("INSERT INTO incoming (person, income) VALUES ('anna', 400000), ('bjorn', 500000), ('anna', 600000), ('nobody', 100)");

        readFeed(engine);

        fixture.awaitQueueEmpty(Duration.ofSeconds(10));
        assertThat(database.rows("SELECT person, notice FROM published_feed ORDER BY person"))
            .containsExactly(List.of("anna", "tax 180000"), List.of("bjorn", "tax 200000"));
        assertThat(database.rows("SELECT identifier, state, output, reference FROM tasks.task_view WHERE topic = 'assessment' ORDER BY id")).containsExactly(
            Arrays.asList("anna", "redundant", null, "income:1"),
            Arrays.asList("bjorn", "succeeded", null, "income:2"),
            Arrays.asList("anna", "succeeded", null, "income:3"),
            Arrays.asList("nobody", "filtered", "unknown person", "income:4")
        );
        assertThat(register.lookups).as("one batched lookup").hasValue(1);
        assertThat(database.rows("SELECT count(*) FROM tasks.task_view WHERE topic = 'archive' AND root_id IN (SELECT id FROM tasks.task_view WHERE topic = 'assessment')"))
            .as("archive tasks belong to the assessment's flow").containsExactly(List.of(2L));
    }

    @Test
    @DisplayName("Retrying already-applied work is safe and still creates the next task")
    void retry_after_the_change_was_applied() throws Exception {
        PartsRegister register = new PartsRegister().register("anna", 30);
        AssessmentLedger ledger = new AssessmentLedger(database.dataSource());
        AssessmentHandler handler = new AssessmentHandler(register, ledger).afterChange(task -> {
            if (task.attempt() == 1) {
                throw new IllegalStateException("crashed after the change, before completing");
            }
        });
        TaskEngine engine = fixture.start("node-a", slice(handler, new FeedPublisher()));

        engine.tasks().independently().push(Slice.ASSESSMENT, "anna", new Slice.AssessmentRequest("anna", 500000));

        fixture.awaitQueueEmpty(Duration.ofSeconds(10));
        assertThat(ledger.calls).hasValue(2);
        assertThat(fixture.count("SELECT count(*) FROM assessment")).isEqualTo(1);
        assertThat(fixture.count("SELECT count(*) FROM tasks.task_view WHERE topic = 'notify'")).isEqualTo(1);
        assertThat(database.rows("SELECT state, attempt FROM tasks.task_view WHERE topic = 'assessment'")).containsExactly(List.of("succeeded", 2));
        assertThat(database.rows("SELECT attempt, error->>'message' FROM tasks.attempt")).containsExactly(List.of(1, "crashed after the change, before completing"));
    }

    @Test
    @DisplayName("A failure retries and finally dead-letters with its error")
    void failure_retries_then_dead_letters() throws Exception {
        TopicRef<String> topic = TopicRef.of("flaky", String.class);
        TaskEngine engine = fixture.start("node-a", List.of(Topic.of(topic, batch -> batch.decide(task -> Decision.failed("still broken")))
            .with(TopicSettings.defaults().withRetry(new Retry(3, Duration.ofMillis(50), 2, Duration.ofMillis(200), 0)))));

        engine.tasks().independently().push(topic, "x", "payload");

        await("the dead letter", Duration.ofSeconds(10), () -> fixture.count("SELECT count(*) FROM tasks.task_view WHERE state = 'failed'") == 1);
        assertThat(database.rows("SELECT attempt, error->>'message' FROM tasks.task_view")).containsExactly(List.of(3, "still broken"));
        assertThat(fixture.count("SELECT count(*) FROM tasks.attempt WHERE result = 'failed'")).isEqualTo(3);
        Thread.sleep(300);
        assertThat(fixture.count("SELECT count(*) FROM tasks.attempt")).as("never retried after that").isEqualTo(3);
    }

    @Test
    @DisplayName("later() runs the task again without using up an attempt")
    void later_recovers_without_using_an_attempt() throws Exception {
        TopicRef<String> topic = TopicRef.of("waiting", String.class);
        AtomicInteger runs = new AtomicInteger();
        TaskEngine engine = fixture.start("node-a", List.of(Topic.of(topic, batch -> batch.decide(task ->
            runs.incrementAndGet() < 3 ? Decision.later(Duration.ofMillis(100)) : Decision.success()))));

        engine.tasks().independently().push(topic, "x", "payload");

        fixture.awaitQueueEmpty(Duration.ofSeconds(10));
        assertThat(runs).hasValue(3);
        assertThat(database.rows("SELECT state, attempt FROM tasks.task_view")).containsExactly(List.of("succeeded", 1));
        assertThat(fixture.count("SELECT count(*) FROM tasks.attempt")).isZero();
    }

    @Test
    @DisplayName("Work is found without a notification, by the fallback poll")
    void wake_up_without_a_notification() throws Exception {
        TopicRef<String> topic = TopicRef.of("polled", String.class);
        CountDownLatch done = new CountDownLatch(1);
        TaskEngine engine = fixture.start("node-a", List.of(Topic.of(topic, batch -> batch.decide(task -> {
            done.countDown();
            return Decision.success();
        })).with(TopicSettings.defaults().withPollInterval(Duration.ofMillis(300)))), settings -> settings.withNotifier(null));
        Thread.sleep(100); // the first, empty poll has happened; the worker now waits

        try (Connection connection = database.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            engine.tasks().on(connection).push(topic, "x", "payload");
            connection.commit();
        }

        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @DisplayName("A push from another node wakes the worker through NOTIFY at commit")
    void notification_wakes_another_node() throws Exception {
        TopicRef<String> topic = TopicRef.of("notified", String.class);
        CountDownLatch done = new CountDownLatch(1);
        fixture.start("consumer", List.of(Topic.of(topic, batch -> batch.decide(task -> {
            done.countDown();
            return Decision.success();
        })).with(TopicSettings.defaults().withPollInterval(Duration.ofHours(1)))));
        TaskEngine producer = fixture.start("producer", List.of());
        Thread.sleep(200);

        try (Connection connection = database.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            producer.tasks().on(connection).push(topic, "x", "payload");
            Thread.sleep(300);
            assertThat(done.getCount()).as("nothing happens before the commit").isEqualTo(1);
            connection.commit();
        }

        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @DisplayName("A batch that runs past its deadline is cancelled and retried")
    void batch_deadline() throws Exception {
        TopicRef<String> topic = TopicRef.of("slow", String.class);
        AtomicInteger runs = new AtomicInteger();
        CountDownLatch interrupted = new CountDownLatch(1);
        TaskEngine engine = fixture.start("node-a", List.of(Topic.of(topic, batch -> batch.decide(task -> {
            if (runs.incrementAndGet() == 1) {
                try {
                    Thread.sleep(Duration.ofSeconds(30));
                } catch (InterruptedException e) {
                    interrupted.countDown();
                    throw e;
                }
            }
            return Decision.success();
        })).with(TopicSettings.defaults().withBatchDeadline(Duration.ofMillis(300)).withRetry(FAST))));

        engine.tasks().independently().push(topic, "x", "payload");

        fixture.awaitQueueEmpty(Duration.ofSeconds(10));
        assertThat(interrupted.await(1, TimeUnit.SECONDS)).as("the slow handler was interrupted").isTrue();
        assertThat(database.rows("SELECT attempt, error->>'message' FROM tasks.attempt")).containsExactly(List.of(1, "Batch deadline of PT0.3S exceeded"));
        assertThat(database.rows("SELECT state, attempt FROM tasks.task_view")).containsExactly(List.of("succeeded", 2));
    }

    @Test
    @DisplayName("Stopping waits for running batches, and releases hung ones without using an attempt")
    void stop_drains_then_releases() throws Exception {
        TopicRef<String> topic = TopicRef.of("draining", String.class);
        CountDownLatch started = new CountDownLatch(2);
        TaskEngine engine = fixture.start("node-a", List.of(Topic.of(topic, batch -> batch.decide(task -> {
            started.countDown();
            if (task.identifier().equals("hung")) {
                Thread.sleep(Duration.ofMinutes(5));
            } else {
                Thread.sleep(200);
            }
            return Decision.success();
        })).with(TopicSettings.defaults().withBatchSize(1))));
        engine.tasks().independently().push(topic, "quick", "payload");
        engine.tasks().independently().push(topic, "hung", "payload");
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        engine.stop(Duration.ofMillis(500));

        assertThat(database.rows("SELECT identifier, state, attempt FROM tasks.task_view ORDER BY id"))
            .containsExactly(List.of("quick", "succeeded", 1), List.of("hung", "retrying", 0));
        assertThat(fixture.count("SELECT count(*) FROM tasks.attempt")).isZero();
    }
}
