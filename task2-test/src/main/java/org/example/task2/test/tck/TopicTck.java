package org.example.task2.test.tck;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.example.task2.spi.TaskStore;
import org.junit.jupiter.api.Test;

/** TCK: the registered topics and pausing them. */
public abstract class TopicTck extends StoreTck {

    private static List<TaskStore.TopicInfo> topics(StoreFixture fixture, String... names) throws SQLException {
        // Only the test's own topics: a store's test may keep others in the background.
        List<String> wanted = List.of(names);
        return fixture.inTransaction(transaction -> fixture.store().topics(transaction)).stream()
            .filter(topic -> wanted.contains(topic.name()))
            .toList();
    }

    private static boolean pause(StoreFixture fixture, String name, boolean paused) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().setPaused(transaction, name, paused));
    }

    @Test
    void lists_every_topic_with_its_id() throws SQLException {
        StoreFixture fixture = fixture();
        int first = fixture.topic("first"), second = fixture.topic("second");

        assertThat(topics(fixture, "first", "second")).containsExactly(
            new TaskStore.TopicInfo(first, "first", false),
            new TaskStore.TopicInfo(second, "second", false));
    }

    @Test
    void a_topic_can_be_paused_and_resumed() throws SQLException {
        StoreFixture fixture = fixture();
        int topic = fixture.topic("topic");

        assertThat(pause(fixture, "topic", true)).isTrue();
        assertThat(topics(fixture, "topic")).containsExactly(new TaskStore.TopicInfo(topic, "topic", true));
        assertThat(pause(fixture, "topic", true)).as("pausing twice is fine").isTrue();
        assertThat(pause(fixture, "topic", false)).isTrue();
        assertThat(topics(fixture, "topic")).containsExactly(new TaskStore.TopicInfo(topic, "topic", false));
    }

    @Test
    void pausing_an_unknown_topic_says_so() throws SQLException {
        assertThat(pause(fixture(), "missing", true)).isFalse();
    }

    @Test
    void a_paused_topic_still_takes_pushes() throws SQLException {
        StoreFixture fixture = fixture();
        int topic = fixture.topic("topic");
        pause(fixture, "topic", true);

        long id = fixture.push(topic, "a");

        assertThat(fixture.state(id)).isEqualTo("ready");
    }

    @Test
    void a_pause_in_a_rolled_back_transaction_is_not_kept() throws SQLException {
        StoreFixture fixture = fixture();
        fixture.topic("topic");
        try (var transaction = fixture.store().begin()) {
            fixture.store().setPaused(transaction, "topic", true);
            transaction.rollback();
        }
        assertThat(topics(fixture, "topic").getFirst().paused()).isFalse();
    }
}
