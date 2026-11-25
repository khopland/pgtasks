package io.github.khopland.pgtasks.test.tck;

import java.sql.SQLException;
import java.util.List;

import io.github.khopland.pgtasks.spi.TaskStore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** TCK: the registered topics, and deleting one. */
public abstract class TopicTck extends StoreTck {

    private static List<TaskStore.TopicInfo> topics(StoreFixture fixture, String... names) throws SQLException {
        // Only the test's own topics: a store's test may keep others in the background.
        List<String> wanted = List.of(names);
        return fixture.inTransaction(transaction -> fixture.store().topics(transaction)).stream()
            .filter(topic -> wanted.contains(topic.name()))
            .toList();
    }

    @Test
    void lists_every_topic_with_its_id() throws SQLException {
        StoreFixture fixture = fixture();
        int first = fixture.topic("first"), second = fixture.topic("second");

        assertThat(topics(fixture, "first", "second")).containsExactly(
            new TaskStore.TopicInfo(first, "first"),
            new TaskStore.TopicInfo(second, "second"));
    }

    @Test
    void a_topic_without_tasks_is_deleted() throws SQLException {
        StoreFixture fixture = fixture();
        int topic = fixture.topic("retired");
        int node = fixture.node("node");
        long id = fixture.push(topic, "a");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.succeeded());

        boolean withHistory = fixture.inTransaction(transaction -> fixture.store().deleteTopic(transaction, topic));
        assertThat(withHistory).as("history left").isFalse();
        int purged = fixture.inTransaction(transaction -> fixture.store().purgeAll(transaction, topic, 10));
        assertThat(purged).isEqualTo(1);
        assertThat(fixture.view(id)).isEmpty();
        boolean deleted = fixture.inTransaction(transaction -> fixture.store().deleteTopic(transaction, topic));
        assertThat(deleted).isTrue();

        assertThat(topics(fixture, "retired")).isEmpty();
    }

    @Test
    void a_topic_with_an_unfinished_task_is_not_deleted() throws SQLException {
        StoreFixture fixture = fixture();
        int topic = fixture.topic("busy");
        long id = fixture.push(topic, "a");

        int purged = fixture.inTransaction(transaction -> fixture.store().purgeAll(transaction, topic, 10));
        assertThat(purged).as("unfinished stays").isZero();
        boolean deleted = fixture.inTransaction(transaction -> fixture.store().deleteTopic(transaction, topic));
        assertThat(deleted).isFalse();
        assertThat(topics(fixture, "busy")).hasSize(1);
        assertThat(fixture.state(id)).isEqualTo("ready");
    }
}
