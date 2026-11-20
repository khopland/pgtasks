package org.example.task2.test.tck;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import org.example.task2.Insertion;
import org.example.task2.Ordering;
import org.example.task2.spi.TaskStore;
import org.junit.jupiter.api.Test;

/** TCK: the strict rate limit, a token bucket shared by every node. */
public abstract class AdmissionTck extends StoreTck {

    private final TaskStore.Admission limit = new TaskStore.Admission("downstream", 10, 3);
    private int topic, node;

    private StoreFixture setUp() throws SQLException {
        StoreFixture fixture = fixture();
        topic = fixture.topic("topic");
        node = fixture.node("node-a");
        fixture.inTransaction(transaction -> {
            fixture.store().ensureAdmission(transaction, limit.name(), limit.burst());
            return null;
        });
        return fixture;
    }

    private TaskStore.Claim claim(StoreFixture fixture, int node, int size) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().claim(transaction, new TaskStore.ClaimRequest(topic, node, Ordering.FIFO, size, limit)));
    }

    @Test
    void takes_permits_before_polling_and_waits_when_none_are_left() throws SQLException {
        StoreFixture fixture = setUp();
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 20));

        TaskStore.Claim first = claim(fixture, node, 100);
        TaskStore.Claim second = claim(fixture, node, 100);

        assertThat(first.tasks()).as("the burst").hasSize(3);
        assertThat(second.tasks()).isEmpty();
        assertThat(second.admissionWait()).isPositive().isLessThanOrEqualTo(Duration.ofMillis(110));
        assertThat(fixture.counts(topic).active()).isEqualTo(3);
    }

    @Test
    void empty_polls_are_not_charged() throws SQLException {
        StoreFixture fixture = setUp();
        assertThat(claim(fixture, node, 100).tasks()).isEmpty();
        assertThat(claim(fixture, node, 100).tasks()).isEmpty();
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 5));

        assertThat(claim(fixture, node, 100).tasks()).hasSize(3);
    }

    @Test
    void refills_at_the_configured_rate() throws Exception {
        StoreFixture fixture = setUp();
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 20));
        assertThat(claim(fixture, node, 100).tasks()).hasSize(3);

        fixture.advance(Duration.ofMillis(250));

        assertThat(claim(fixture, node, 100).tasks()).hasSizeBetween(2, 3);
    }

    @Test
    void never_holds_more_than_the_burst() throws Exception {
        StoreFixture fixture = setUp();
        fixture.advance(Duration.ofHours(1));
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 20));

        assertThat(claim(fixture, node, 100).tasks()).hasSize(3);
    }

    @Test
    void the_bucket_is_shared_by_every_node() throws SQLException {
        StoreFixture fixture = setUp();
        int other = fixture.node("node-b");
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 20));

        List<TaskStore.ClaimedTask> mine = claim(fixture, node, 2).tasks();
        List<TaskStore.ClaimedTask> theirs = claim(fixture, other, 100).tasks();

        assertThat(mine.size() + theirs.size()).isEqualTo(3);
    }
}
