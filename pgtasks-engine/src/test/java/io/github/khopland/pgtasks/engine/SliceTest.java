package io.github.khopland.pgtasks.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.slice.AssessmentHandler;
import io.github.khopland.pgtasks.slice.AssessmentLedger;
import io.github.khopland.pgtasks.slice.FeedPublisher;
import io.github.khopland.pgtasks.slice.IncomeFeed;
import io.github.khopland.pgtasks.slice.NoticeHandler;
import io.github.khopland.pgtasks.slice.PartsRegister;
import io.github.khopland.pgtasks.slice.Slice;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.github.khopland.pgtasks.engine.EngineFixture.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The checklist from the infra guide's "Testing a task flow", applied to the slice on Postgres:
 * the parts about SQL, NOTIFY and JDBC transactions. Retries, delays, deadlines and stopping are
 * covered on the in-memory store in EngineTest.
 */
class SliceTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final Retry FAST = Retry.exponential(Duration.ofMillis(50), Duration.ofMillis(400)).attempts(5).withJitter(0.5);

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
        assertThat(database.rows("SELECT identifier, state, output, reference FROM pgtasks.task_view WHERE topic = 'assessment' ORDER BY id")).containsExactly(
            Arrays.asList("anna", "redundant", null, "income:1"),
            Arrays.asList("bjorn", "succeeded", null, "income:2"),
            Arrays.asList("anna", "succeeded", null, "income:3"),
            Arrays.asList("nobody", "filtered", "unknown person", "income:4")
        );
        assertThat(register.lookups).as("one batched lookup").hasValue(1);
        assertThat(database.rows("SELECT count(*) FROM pgtasks.task_view WHERE topic = 'archive' AND root_id IN (SELECT id FROM pgtasks.task_view WHERE topic = 'assessment')"))
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
        assertThat(fixture.count("SELECT count(*) FROM pgtasks.task_view WHERE topic = 'notify'")).isEqualTo(1);
        assertThat(database.rows("SELECT state, attempt FROM pgtasks.task_view WHERE topic = 'assessment'")).containsExactly(List.of("succeeded", 2));
        assertThat(database.rows("SELECT attempt, error->>'message' FROM pgtasks.attempt")).containsExactly(List.of(1, "crashed after the change, before completing"));
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
}
