package org.example.task2.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import org.example.task2.Insertion;
import org.example.task2.Ordering;
import org.example.task2.spi.TaskStore;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class AdmissionTest {

    @RegisterExtension
    static PostgresExtension postgres = StoreFixture.extension();

    private StoreFixture fixture;
    private int topic, node;
    private final TaskStore.Admission limit = new TaskStore.Admission("downstream", 10, 3);

    @BeforeEach
    void setUp(PostgresDatabase database) throws SQLException {
        fixture = new StoreFixture(database);
        topic = fixture.topic("topic");
        node = fixture.node("node-a");
    }

    private TaskStore.Claim claim(int size) throws SQLException {
        return fixture.inTransaction(connection -> fixture.store.claim(connection, new TaskStore.ClaimRequest(topic, node, Ordering.FIFO, size, limit)));
    }

    @Test
    void takes_permits_before_polling_and_waits_when_none_are_left() throws SQLException {
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 20));

        TaskStore.Claim first = claim(100);
        TaskStore.Claim second = claim(100);

        assertThat(first.tasks()).as("the burst").hasSize(3);
        assertThat(second.tasks()).isEmpty();
        assertThat(second.admissionWait()).isPositive().isLessThanOrEqualTo(Duration.ofMillis(110));
        assertThat(fixture.database.count("SELECT count(*) FROM tasks.queue WHERE state = 'active'")).isEqualTo(3);
    }

    @Test
    void empty_polls_are_not_charged() throws SQLException {
        assertThat(claim(100).tasks()).isEmpty();
        assertThat(claim(100).tasks()).isEmpty();
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 5));

        assertThat(claim(100).tasks()).hasSize(3);
    }

    @Test
    void refills_at_the_configured_rate() throws Exception {
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 20));
        assertThat(claim(100).tasks()).hasSize(3);

        Thread.sleep(250);

        assertThat(claim(100).tasks()).hasSizeBetween(2, 3);
    }

    @Test
    void the_bucket_is_shared_by_every_node() throws SQLException {
        int other = fixture.node("node-b");
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 20));

        List<TaskStore.ClaimedTask> mine = claim(2).tasks();
        List<TaskStore.ClaimedTask> theirs = fixture.inTransaction(connection ->
            fixture.store.claim(connection, new TaskStore.ClaimRequest(topic, other, Ordering.FIFO, 100, limit)).tasks());

        assertThat(mine.size() + theirs.size()).isEqualTo(3);
    }
}
