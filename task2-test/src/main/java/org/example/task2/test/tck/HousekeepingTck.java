package org.example.task2.test.tck;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.example.task2.Insertion;
import org.example.task2.Outcome;
import org.example.task2.spi.TaskStore;
import org.junit.jupiter.api.Test;

/** TCK: leadership of cluster-wide jobs, retention and count snapshots. */
public abstract class HousekeepingTck extends StoreTck {

    @Test
    void one_leader_at_a_time() throws Exception {
        StoreFixture fixture = fixture();
        try (TaskStore.Lease first = fixture.store().lead("housekeeping");
             TaskStore.Lease second = fixture.store().lead("housekeeping");
             TaskStore.Lease other = fixture.store().lead("something-else")) {
            assertThat(first.held()).isTrue();
            assertThat(second.held()).isFalse();
            assertThat(other.held()).isTrue();

            first.close();
            assertThat(first.held()).isFalse();
            try (TaskStore.Lease next = fixture.store().lead("housekeeping")) {
                assertThat(next.held()).as("the lease passes on once released").isTrue();
            }
        }
    }

    @Test
    void retention_deletes_only_finished_tasks_with_that_outcome_completed_before_the_cutoff() throws Exception {
        StoreFixture fixture = fixture();
        int topic = fixture.topic("topic");
        int node = fixture.node("node-a");
        long old = fixture.push(topic, "old");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.succeeded());
        long filtered = fixture.push(topic, "filtered");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.filtered("x"));
        long dead = fixture.push(topic, "dead");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.deadLetter());
        long waiting = fixture.push(topic, "waiting");
        Instant cutoff = fixture.store().clock().instant().plusSeconds(1);
        fixture.advance(Duration.ofSeconds(2));
        long recent = fixture.push(topic, "recent");
        fixture.complete(topic, node, fixture.claim(topic, node).stream().filter(task -> task.id() == recent).findFirst().orElseThrow(), StoreFixture.succeeded());

        int deleted = fixture.inTransaction(transaction -> fixture.store().purge(transaction, topic, Outcome.SUCCEEDED, cutoff.plusSeconds(60), 100));

        assertThat(deleted).isEqualTo(2);
        assertThat(fixture.state(old)).isNull();
        assertThat(fixture.state(recent)).as("completed after the cutoff in store time, then backdated").isNull();
        assertThat(fixture.state(filtered)).isEqualTo("filtered");
        assertThat(fixture.state(dead)).as("dead letters are unfinished").isEqualTo("failed");
        assertThat(fixture.state(waiting)).isNotNull();
    }

    @Test
    void retention_respects_the_cutoff_and_the_limit() throws Exception {
        StoreFixture fixture = fixture();
        int topic = fixture.topic("topic");
        int node = fixture.node("node-a");
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 5));
        for (var task : fixture.claim(topic, node)) {
            fixture.complete(topic, node, task, StoreFixture.succeeded());
        }
        Instant beforeCompletion = fixture.store().clock().instant().minusSeconds(60);

        int none = fixture.inTransaction(transaction -> fixture.store().purge(transaction, topic, Outcome.SUCCEEDED, beforeCompletion, 100));
        int limited = fixture.inTransaction(transaction -> fixture.store().purge(transaction, topic, Outcome.SUCCEEDED, Instant.now().plusSeconds(3600), 2));
        int rest = fixture.inTransaction(transaction -> fixture.store().purge(transaction, topic, Outcome.SUCCEEDED, Instant.now().plusSeconds(3600), 100));
        assertThat(none).isZero();
        assertThat(limited).isEqualTo(2);
        assertThat(rest).isEqualTo(3);
    }

    @Test
    void count_snapshots_record_every_topic() throws Exception {
        StoreFixture fixture = fixture();
        int topic = fixture.topic("busy");
        fixture.topic("idle");
        int node = fixture.node("node-a");
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 3));
        fixture.claim(topic, node, org.example.task2.Ordering.FIFO, 1);

        fixture.inTransaction(transaction -> {
            fixture.store().snapshotCounts(transaction);
            return null;
        });
        List<TaskStore.TopicSnapshot> snapshots = fixture.inTransaction(transaction -> fixture.store().snapshots(transaction));

        assertThat(snapshots).extracting(TaskStore.TopicSnapshot::topic).containsExactly("busy", "idle");
        assertThat(snapshots.getFirst().counts().ready()).isEqualTo(2);
        assertThat(snapshots.getFirst().counts().active()).isEqualTo(1);
        assertThat(snapshots.get(1).counts().total()).isZero();
        assertThat(snapshots.getFirst().takenAt()).isNotNull();
    }
}
