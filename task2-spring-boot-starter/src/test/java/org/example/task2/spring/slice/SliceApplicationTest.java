package org.example.task2.spring.slice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.example.task2.Creation;
import org.example.task2.Insertion;
import org.example.task2.Tasks;
import org.example.task2.engine.EngineFixture;
import org.example.task2.slice.AssessmentLedger;
import org.example.task2.slice.FeedPublisher;
import org.example.task2.slice.PartsRegister;
import org.example.task2.slice.Slice;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.TestPostgres;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Phase 4's exit test: the vertical slice runs unchanged as a Spring Boot application. */
@SpringBootTest(classes = SliceApplication.class)
class SliceApplicationTest {

    /** One database for the context; each test starts from empty tables. */
    static final PostgresDatabase DATABASE = TestPostgres.createDatabase();

    static {
        Slice.createDomainTables(DATABASE);
    }

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", DATABASE::jdbcUrl);
        registry.add("spring.datasource.username", DATABASE::user);
        registry.add("spring.datasource.password", DATABASE::password);
    }

    @Autowired
    SliceApplication.IncomeFeedJob feed;
    @Autowired
    Tasks tasks;
    @Autowired
    PartsRegister register;
    @Autowired
    AssessmentLedger ledger;
    @Autowired
    FeedPublisher publisher;

    @BeforeEach
    void emptyTables() throws InterruptedException {
        awaitQueueEmpty();
        DATABASE.execute("""
            TRUNCATE incoming, assessment, published_feed RESTART IDENTITY;
            UPDATE feed_pointer SET position = 0;
            DELETE FROM tasks.attempt;
            DELETE FROM tasks.task;
            """);
        register.lookups.set(0);
        ledger.calls.set(0);
    }

    @Test
    @DisplayName("A normal flow succeeds end to end, with one lookup per batch and newer income superseding older")
    void normal_flow() throws Exception {
        DATABASE.execute("INSERT INTO incoming (person, income) VALUES ('anna', 400000), ('bjorn', 500000), ('anna', 600000), ('nobody', 100)");

        assertThat(feed.readPage(100)).isEqualTo(4);

        awaitQueueEmpty();
        assertThat(DATABASE.rows("SELECT person, notice FROM published_feed ORDER BY person"))
            .containsExactly(List.of("anna", "tax 180000"), List.of("bjorn", "tax 200000"));
        assertThat(DATABASE.rows("SELECT identifier, state, output, reference FROM tasks.task_view WHERE topic = 'assessment' ORDER BY id")).containsExactly(
            Arrays.asList("anna", "redundant", null, "income:1"),
            Arrays.asList("bjorn", "succeeded", null, "income:2"),
            Arrays.asList("anna", "succeeded", null, "income:3"),
            Arrays.asList("nobody", "filtered", "unknown person", "income:4")
        );
        assertThat(register.lookups).as("one batched lookup").hasValue(1);
        assertThat(DATABASE.count("SELECT count(*) FROM tasks.task_view WHERE topic = 'archive'")).isEqualTo(2);
    }

    @Test
    @DisplayName("1. A feed rollback (a @Transactional method throwing) leaves neither a task nor an advanced pointer")
    void feed_rollback_leaves_neither_task_nor_pointer() throws Exception {
        DATABASE.execute("INSERT INTO incoming (person, income) VALUES ('anna', 500000), ('bjorn', 600000)");

        assertThatThrownBy(() -> feed.readPageThenFail(10)).hasMessage("the feed job failed after reading its page");
        Thread.sleep(300);
        assertThat(DATABASE.count("SELECT count(*) FROM tasks.task")).isZero();
        assertThat(DATABASE.count("SELECT position FROM feed_pointer")).isZero();
        assertThat(ledger.calls).hasValue(0);

        feed.readPage(10);
        assertThat(DATABASE.count("SELECT position FROM feed_pointer")).isEqualTo(2);
        awaitQueueEmpty();
        assertThat(DATABASE.count("SELECT count(*) FROM published_feed")).isEqualTo(2);
        assertThat(DATABASE.count("SELECT count(*) FROM tasks.task_view WHERE topic = 'archive'")).isEqualTo(2);
    }

    @Test
    @DisplayName("2. A completion rollback leaves neither a JDBC publication nor a child task, and the retry publishes once")
    void completion_rollback_leaves_neither_publication_nor_child() throws Exception {
        publisher.failures.set(1);

        tasks.independently().push(Slice.NOTIFY, Insertion.APPEND, List.of(
            Creation.of("anna", new Slice.Notice("anna", 150000)),
            Creation.of("bjorn", new Slice.Notice("bjorn", 240000))
        ));

        awaitQueueEmpty();
        assertThat(DATABASE.rows("SELECT DISTINCT result, error->>'message' FROM tasks.attempt"))
            .containsExactly(List.of("failed", "Simulated failure while publishing"));
        assertThat(DATABASE.count("SELECT count(*) FROM published_feed")).isEqualTo(2);
        assertThat(DATABASE.count("SELECT count(*) FROM tasks.task_view WHERE topic = 'archive'")).isEqualTo(2);
        assertThat(DATABASE.rows("SELECT state, attempt FROM tasks.task_view WHERE topic = 'notify'")).containsOnly(List.of("succeeded", 2));
    }

    private static void awaitQueueEmpty() throws InterruptedException {
        EngineFixture.await("an empty queue", Duration.ofSeconds(10), () -> DATABASE.count("SELECT count(*) FROM tasks.queue") == 0);
    }
}
