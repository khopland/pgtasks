package org.example.task2.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.util.List;

import org.example.task2.Creation;
import org.example.task2.Insertion;
import org.example.task2.Pushed;
import org.example.task2.Tasks;
import org.example.task2.TopicRef;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Transaction-aware enqueue: a push joins a transaction, or says clearly that it doesn't. */
class TasksTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final TopicRef<String> TOPIC = TopicRef.of("topic", String.class);

    private record Unregistered(String value) {
    }

    private EngineFixture fixture;
    private JdbcTransactions transactions;
    private Tasks tasks;

    @BeforeEach
    void setUp(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        fixture.createTopics(TOPIC.name());
        transactions = new JdbcTransactions(database.dataSource());
        tasks = fixture.create("producer", List.of(), settings -> settings.withTransactions(transactions)).tasks();
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    private long stored() {
        return fixture.count("SELECT count(*) FROM tasks.task");
    }

    @Test
    void push_without_a_transaction_throws() {
        assertThatThrownBy(() -> tasks.push(TOPIC, "x", "payload"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("tasks.on(connection)")
            .hasMessageContaining("tasks.independently()");
        assertThat(stored()).isZero();
    }

    @Test
    void push_joins_the_current_transaction() throws Exception {
        assertThatThrownBy(() -> transactions.inTransaction(connection -> {
            tasks.push(TOPIC, "x", "payload");
            assertThat(stored()).as("not visible before the commit").isZero();
            throw new IllegalStateException("business logic failed");
        })).hasMessage("business logic failed");
        assertThat(stored()).isZero();

        transactions.inTransaction(connection -> tasks.push(TOPIC, "x", "payload"));
        assertThat(stored()).isEqualTo(1);
    }

    @Test
    void on_connection_joins_its_transaction_and_refuses_auto_commit() throws Exception {
        try (Connection connection = fixture.database.dataSource().getConnection()) {
            assertThatThrownBy(() -> tasks.on(connection).push(TOPIC, "x", "payload"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("auto-commit");

            connection.setAutoCommit(false);
            tasks.on(connection).push(TOPIC, "x", "payload");
            connection.rollback();
            assertThat(stored()).isZero();

            tasks.on(connection).push(TOPIC, "x", "payload");
            connection.commit();
            assertThat(stored()).isEqualTo(1);
        }
    }

    @Test
    void independently_commits_on_its_own() throws Exception {
        assertThatThrownBy(() -> transactions.inTransaction(connection -> {
            tasks.independently().push(TOPIC, "x", "payload");
            throw new IllegalStateException("the surrounding transaction rolls back");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(stored()).isEqualTo(1);
    }

    @Test
    void unknown_topics_are_rejected_instead_of_created() {
        assertThatThrownBy(() -> tasks.independently().push(TopicRef.of("tpoic", String.class), "x", "payload"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown topic 'tpoic'");
        assertThat(fixture.count("SELECT count(*) FROM tasks.topic")).isEqualTo(1);
    }

    @Test
    void a_payload_type_without_a_codec_is_rejected() {
        assertThatThrownBy(() -> tasks.independently().push(TopicRef.of("topic", Unregistered.class), "x", new Unregistered("v")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No payload codec");
    }

    @Test
    void dedup_keys_and_insertion_policies_are_reported() {
        List<Pushed> pushed = tasks.independently().push(TOPIC, Insertion.SUPERSEDE, List.of(
            Creation.of("a", "first"), Creation.of("a", "second").withDedupKey("k")
        ));
        Pushed duplicate = tasks.independently().push(TOPIC, Insertion.APPEND, Creation.of("b", "x").withDedupKey("k"));

        assertThat(pushed).extracting(Pushed::status).containsExactly(Pushed.Status.REDUNDANT, Pushed.Status.QUEUED);
        assertThat(duplicate.status()).isEqualTo(Pushed.Status.DUPLICATE);
        assertThat(duplicate.findId()).isEmpty();
    }
}
