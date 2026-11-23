package io.github.khopland.pgtasks.engine;

import java.sql.Connection;
import java.util.List;
import java.util.OptionalInt;

import io.github.khopland.pgtasks.TaskStorageException;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.TaskQuery;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Review 2026-11-23: with topicsCreatedOnPush, a topic created inside a transaction that rolled
 * back stayed cached by id, and a push-only node, which has no heartbeat to forget it, failed every
 * later push to it on the foreign key until restarted.
 */
class TopicsCreatedOnPushTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final TopicRef<String> FRESH = TopicRef.of("fresh", String.class);

    private EngineFixture fixture;

    @BeforeEach
    void setUp(PostgresDatabase database) {
        fixture = new EngineFixture(database);
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    private long stored(String topic) {
        return fixture.count("SELECT count(*) FROM pgtasks.task t JOIN pgtasks.topic p ON p.id = t.topic_id WHERE p.name = ?", topic);
    }

    @Test
    void a_push_only_node_keeps_pushing_after_the_transaction_that_created_the_topic_rolled_back() throws Exception {
        TaskEngine producer = fixture.create("producer", List.of(), settings -> settings).withTopicsCreatedOnPush().prepare();
        assertThat(producer.running()).isFalse();
        try (Connection connection = fixture.database.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            producer.tasks().on(connection).push(FRESH, "a", "x");
            connection.rollback();
            assertThat(fixture.count("SELECT count(*) FROM pgtasks.topic WHERE name = 'fresh'"))
                .as("created in a transaction of its own, so the rollback keeps it").isEqualTo(1);

            producer.tasks().on(connection).push(FRESH, "b", "y");
            connection.commit();
        }
        assertThat(stored("fresh")).isEqualTo(1);
    }

    @Test
    void a_push_only_node_looks_a_topic_up_again_once_it_was_deleted_elsewhere() throws Exception {
        fixture.createTopics(FRESH.name());
        TaskEngine producer = fixture.create("producer", List.of(), settings -> settings).prepare();
        TaskEngine operator = fixture.create("operator", List.of(), settings -> settings).prepare();
        producer.tasks().independently().push(FRESH, "a", "x");
        operator.admin().cancel(FRESH.name(), TaskQuery.ALL);
        operator.admin().deleteTopic(FRESH.name());
        fixture.createTopics(FRESH.name());

        // The cached id is stale: this push fails on the foreign key, and drops it.
        assertThatThrownBy(() -> producer.tasks().independently().push(FRESH, "b", "y")).isInstanceOf(TaskStorageException.class);
        producer.tasks().independently().push(FRESH, "c", "z");
        assertThat(stored("fresh")).isEqualTo(1);
    }

    @Test
    void a_topic_created_on_push_is_cached_only_once_committed() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        TopicDirectory directory = new TopicDirectory(store);
        directory.createOnPush();
        int id;
        try (StoreTransaction transaction = store.begin()) {
            id = directory.id(transaction, "fresh");
            transaction.rollback();
        }
        try (StoreTransaction transaction = store.begin()) {
            OptionalInt found = store.findTopic(transaction, "fresh");
            assertThat(found).as("committed on its own, so the cached id names a topic that exists").hasValue(id);
            assertThat(directory.id(transaction, "fresh")).isEqualTo(id);
        }
    }

    @Test
    void a_cached_id_is_forgotten_only_if_it_is_still_the_one_that_failed() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        TopicDirectory directory = new TopicDirectory(store);
        directory.register("fresh", 7);
        directory.forget("fresh", 8);
        assertThat(directory.known("fresh")).isTrue();
        directory.forget("fresh", 7);
        assertThat(directory.known("fresh")).isFalse();
    }
}
