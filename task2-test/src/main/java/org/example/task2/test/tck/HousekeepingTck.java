package org.example.task2.test.tck;

import static org.assertj.core.api.Assertions.assertThat;

import org.example.task2.admin.TopicSnapshot;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.List;
import java.util.Map;

import org.example.task2.Insertion;
import org.example.task2.Outcome;
import org.example.task2.spi.TaskStore;
import org.junit.jupiter.api.Test;

/** TCK: leadership of cluster-wide jobs, retention and count snapshots. */
public abstract class HousekeepingTck extends StoreTck {

    private static final Duration TERM = Duration.ofSeconds(10);

    private static boolean lead(StoreFixture fixture, String name, int node) throws Exception {
        return fixture.inTransaction(transaction -> fixture.store().lead(transaction, name, node, TERM));
    }

    @Test
    void one_leader_at_a_time() throws Exception {
        StoreFixture fixture = fixture();
        int nodeA = fixture.node("node-a"), nodeB = fixture.node("node-b");

        assertThat(lead(fixture, "housekeeping", nodeA)).isTrue();
        assertThat(lead(fixture, "housekeeping", nodeB)).as("held by A").isFalse();
        assertThat(lead(fixture, "something-else", nodeB)).as("another lease").isTrue();
        assertThat(lead(fixture, "housekeeping", nodeA)).as("A renews").isTrue();

        fixture.inTransaction(transaction -> {
            fixture.store().resign(transaction, "housekeeping", nodeB);
            return null;
        });
        assertThat(lead(fixture, "housekeeping", nodeB)).as("B can't resign A's lease").isFalse();
        fixture.inTransaction(transaction -> {
            fixture.store().resign(transaction, "housekeeping", nodeA);
            return null;
        });
        assertThat(lead(fixture, "housekeeping", nodeB)).as("free once A resigned").isTrue();
        assertThat(lead(fixture, "housekeeping", nodeA)).isFalse();
    }

    @Test
    void a_lease_whose_term_ran_out_goes_to_the_next_node() throws Exception {
        StoreFixture fixture = fixture();
        int nodeA = fixture.node("node-a"), nodeB = fixture.node("node-b");
        assertThat(lead(fixture, "housekeeping", nodeA)).isTrue();

        fixture.advance(TERM.minusSeconds(2));
        assertThat(lead(fixture, "housekeeping", nodeB)).as("A's term hasn't run out").isFalse();

        fixture.advance(Duration.ofSeconds(4));
        assertThat(lead(fixture, "housekeeping", nodeB)).as("A stopped renewing").isTrue();
        assertThat(lead(fixture, "housekeeping", nodeA)).as("A lost it").isFalse();
    }

    @Test
    void a_lease_taken_in_a_rolled_back_transaction_is_not_held() throws Exception {
        StoreFixture fixture = fixture();
        int nodeA = fixture.node("node-a"), nodeB = fixture.node("node-b");
        try (var transaction = fixture.store().begin()) {
            assertThat(fixture.store().lead(transaction, "housekeeping", nodeA, TERM)).isTrue();
            transaction.rollback();
        }
        assertThat(lead(fixture, "housekeeping", nodeB)).isTrue();
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
    void retention_is_recorded_per_topic_and_the_last_setting_wins() throws Exception {
        StoreFixture fixture = fixture();
        int first = fixture.topic("first"), second = fixture.topic("second");
        fixture.topic("unset");
        int empty = fixture.topic("empty");
        Map<Outcome, Period> kept = Map.of(Outcome.SUCCEEDED, Period.ofDays(30), Outcome.FILTERED, Period.ofWeeks(1));
        fixture.inTransaction(transaction -> {
            fixture.store().setRetention(transaction, first, kept);
            fixture.store().setRetention(transaction, second, Map.of(Outcome.SUCCEEDED, Period.ofYears(1)));
            fixture.store().setRetention(transaction, second, Map.of(Outcome.FAILED, Period.ofMonths(18)));
            fixture.store().setRetention(transaction, empty, Map.of());
            return null;
        });

        Map<Integer, Map<Outcome, Period>> retention = fixture.inTransaction(transaction -> fixture.store().retention(transaction));

        assertThat(retention).as("topics without retention keep everything and aren't listed").containsOnlyKeys(first, second);
        assertThat(retention.get(first)).isEqualTo(kept);
        assertThat(retention.get(second)).isEqualTo(Map.of(Outcome.FAILED, Period.ofMonths(18)));
    }

    @Test
    void retention_set_in_a_rolled_back_transaction_is_not_recorded() throws Exception {
        StoreFixture fixture = fixture();
        int topic = fixture.topic("topic");
        try (var transaction = fixture.store().begin()) {
            fixture.store().setRetention(transaction, topic, Map.of(Outcome.SUCCEEDED, Period.ofDays(1)));
            transaction.rollback();
        }
        Map<Integer, Map<Outcome, Period>> retention = fixture.inTransaction(transaction -> fixture.store().retention(transaction));
        assertThat(retention).isEmpty();
    }

    @Test
    void nodes_dead_for_long_and_holding_nothing_are_forgotten() throws Exception {
        StoreFixture fixture = fixture();
        int topic = fixture.topic("topic");
        int idle = fixture.node("idle"), claiming = fixture.node("claiming"), alive = fixture.node("alive");
        fixture.push(topic, "a");
        fixture.claim(topic, claiming);
        fixture.advance(Duration.ofDays(4));
        fixture.heartbeat(alive);

        int deleted = fixture.inTransaction(transaction -> fixture.store().deleteDeadNodes(transaction, Duration.ofDays(3)));

        assertThat(deleted).as("only the idle one: the other still holds a claim").isEqualTo(1);
        int again = fixture.inTransaction(transaction -> fixture.store().deleteDeadNodes(transaction, Duration.ofDays(3)));
        assertThat(again).isZero();
        assertThat(idle).isNotEqualTo(alive);
    }

    @Test
    void count_snapshots_record_every_topic() throws Exception {
        StoreFixture fixture = fixture();
        int topic = fixture.topic("busy");
        fixture.topic("idle");
        int node = fixture.node("node-a");
        List<org.example.task2.Pushed> pushed = fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 3));
        fixture.claim(topic, node, org.example.task2.Ordering.FIFO, 1);

        fixture.inTransaction(transaction -> {
            fixture.store().snapshotCounts(transaction);
            return null;
        });
        // Only this test's topics: a store's test may keep others in the background.
        List<TopicSnapshot> snapshots = fixture.inTransaction(transaction -> fixture.store().snapshots(transaction)).stream()
            .filter(snapshot -> List.of("busy", "idle").contains(snapshot.topic()))
            .toList();

        assertThat(snapshots).extracting(TopicSnapshot::topic).containsExactly("busy", "idle");
        assertThat(snapshots.getFirst().counts().ready()).isEqualTo(2);
        assertThat(snapshots.getFirst().counts().active()).isEqualTo(1);
        assertThat(snapshots.get(1).counts().total()).isZero();
        assertThat(snapshots.getFirst().takenAt()).isNotNull();
        // The first task is claimed, so the oldest ready one is the second.
        assertThat(snapshots.getFirst().counts().oldestReadyAt()).isEqualTo(fixture.view(pushed.get(1).id()).orElseThrow().createdAt());
        assertThat(snapshots.get(1).counts().oldestReadyAt()).isNull();
    }
}
