package org.example.task2.test.tck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.example.task2.test.tck.StoreFixture.completion;
import static org.example.task2.test.tck.StoreFixture.task;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.example.task2.Insertion;
import org.example.task2.spi.StaleClaimException;
import org.example.task2.spi.TaskStore;
import org.example.task2.spi.TaskStore.AttemptView;
import org.example.task2.spi.TaskStore.ClaimedTask;
import org.example.task2.spi.TaskStore.TaskView;
import org.junit.jupiter.api.Test;

/** TCK: fenced completion, retries, dead letters, promotion and expiry. */
public abstract class CompletionTck extends StoreTck {

    private int topic, node;

    private StoreFixture setUp() throws SQLException {
        StoreFixture fixture = fixture();
        topic = fixture.topic("topic");
        node = fixture.node("node-a");
        return fixture;
    }

    @Test
    void success_removes_the_task_from_the_queue_and_records_the_outcome() throws SQLException {
        StoreFixture fixture = setUp();
        long id = fixture.push(topic, "a");
        ClaimedTask claimed = fixture.claim(topic, node).getFirst();

        fixture.complete(topic, node, claimed, StoreFixture.succeeded());

        TaskView view = fixture.view(id).orElseThrow();
        assertThat(view.state()).isEqualTo("succeeded");
        assertThat(view.finished()).isTrue();
        assertThat(view.attempt()).isEqualTo(1);
        assertThat(view.completedAt()).isNotNull();
        assertThat(fixture.counts(topic).total()).isZero();
    }

    @Test
    void filtered_keeps_its_reason() throws SQLException {
        StoreFixture fixture = setUp();
        long id = fixture.push(topic, "a");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.filtered("irrelevant"));

        TaskView view = fixture.view(id).orElseThrow();
        assertThat(view.state()).isEqualTo("filtered");
        assertThat(view.output()).isEqualTo("irrelevant");
    }

    @Test
    void a_completion_with_a_stale_attempt_matches_nothing_and_must_roll_back() throws Exception {
        StoreFixture fixture = setUp();
        long id = fixture.push(topic, "a");
        int crashed = fixture.node("crashed");
        ClaimedTask stale = fixture.claim(topic, crashed).getFirst();
        fixture.advance(Duration.ofHours(1));
        fixture.heartbeat(node);
        fixture.requeueExpired(node, Duration.ofMinutes(10));
        ClaimedTask current = fixture.claim(topic, node).getFirst();
        assertThat(current.attempt()).isEqualTo(2);

        assertThatThrownBy(() -> fixture.inTransaction(transaction -> {
            fixture.store().push(transaction, topic, Insertion.APPEND, List.of(task("child-of-stale")));
            fixture.store().complete(transaction, topic, crashed, List.of(completion(stale, StoreFixture.succeeded())));
            return null;
        })).isInstanceOf(StaleClaimException.class).satisfies(e -> assertThat(((StaleClaimException) e).lost()).containsExactly(id));

        assertThat(fixture.state(id)).isEqualTo("active");
        assertThat(fixture.counts(topic).total()).as("the child rolled back with the completion").isEqualTo(1);

        fixture.complete(topic, node, current, StoreFixture.succeeded());
        assertThat(fixture.state(id)).isEqualTo("succeeded");
    }

    @Test
    void a_completion_is_all_or_nothing_across_the_batch() throws SQLException {
        StoreFixture fixture = setUp();
        fixture.push(topic, Insertion.APPEND, task("a"), task("b"));
        List<ClaimedTask> claimed = fixture.claim(topic, node);
        fixture.complete(topic, node, claimed.get(1), StoreFixture.succeeded());

        assertThatThrownBy(() -> fixture.inTransaction(transaction -> {
            fixture.store().complete(transaction, topic, node, claimed.stream().map(c -> completion(c, StoreFixture.filtered("x"))).toList());
            return null;
        })).isInstanceOf(StaleClaimException.class).satisfies(e -> assertThat(((StaleClaimException) e).lost()).containsExactly(claimed.get(1).id()));
        assertThat(fixture.state(claimed.getFirst().id())).isEqualTo("active");
        assertThat(fixture.state(claimed.get(1).id())).isEqualTo("succeeded");

        int released = fixture.inTransaction(transaction -> fixture.store().release(transaction, topic, node,
            claimed.stream().map(c -> completion(c, StoreFixture.retrying(Duration.ofSeconds(1)))).toList()));
        assertThat(released).isEqualTo(1);
        assertThat(fixture.state(claimed.getFirst().id())).isEqualTo("retrying");
    }

    @Test
    void another_nodes_completion_is_fenced_out() throws SQLException {
        StoreFixture fixture = setUp();
        fixture.push(topic, "a");
        ClaimedTask claimed = fixture.claim(topic, node).getFirst();
        int other = fixture.node("node-b");

        assertThatThrownBy(() -> fixture.complete(topic, other, claimed, StoreFixture.succeeded())).isInstanceOf(StaleClaimException.class);
    }

    @Test
    void a_failure_backs_off_counts_and_is_logged() throws Exception {
        StoreFixture fixture = setUp();
        long id = fixture.push(topic, "a");
        ClaimedTask claimed = fixture.claim(topic, node).getFirst();
        Instant before = fixture.store().clock().instant();

        fixture.complete(topic, node, claimed, StoreFixture.retrying(Duration.ofMinutes(1)));

        TaskView view = fixture.view(id).orElseThrow();
        assertThat(view.state()).isEqualTo("retrying");
        assertThat(view.attempt()).isEqualTo(1);
        assertThat(view.nodeId()).isNull();
        assertThat(view.runAt()).isBetween(before.plusSeconds(50), before.plusSeconds(70));
        assertThat(fixture.attempts(id)).singleElement().satisfies(attempt -> {
            assertThat(attempt.attempt()).isEqualTo(1);
            assertThat(attempt.result()).isEqualTo("failed");
            assertThat(attempt.error()).contains("boom");
            assertThat(attempt.nodeId()).isEqualTo(node);
        });
        assertThat(fixture.claim(topic, node)).as("not ready before its time").isEmpty();
        assertThat(fixture.promoteDue()).isEmpty();
        assertThat(fixture.counts(topic).due()).isZero();

        fixture.advance(Duration.ofMinutes(2));
        assertThat(fixture.counts(topic).due()).isEqualTo(1);
        assertThat(fixture.promoteDue()).containsExactly(topic);
        ClaimedTask retry = fixture.claim(topic, node).getFirst();
        assertThat(retry.attempt()).isEqualTo(2);
    }

    @Test
    void promotion_respects_its_limit() throws Exception {
        StoreFixture fixture = setUp();
        fixture.push(topic, Insertion.APPEND, StoreFixture.delayed("a", Duration.ofSeconds(10)), StoreFixture.delayed("b", Duration.ofSeconds(10)),
            StoreFixture.delayed("c", Duration.ofSeconds(10)), StoreFixture.delayed("d", Duration.ofSeconds(10)), StoreFixture.delayed("e", Duration.ofSeconds(10)));
        fixture.advance(Duration.ofSeconds(20));

        List<Integer> promoted = fixture.inTransaction(transaction -> fixture.store().promoteDue(transaction, 2));
        assertThat(promoted).containsExactly(topic);
        assertThat(fixture.counts(topic).ready()).isEqualTo(2);
        assertThat(fixture.counts(topic).due()).isEqualTo(3);

        assertThat(fixture.promoteDue()).containsExactly(topic);
        assertThat(fixture.counts(topic).ready()).isEqualTo(5);
    }

    @Test
    void a_dead_letter_stays_in_the_queue_with_its_error() throws SQLException {
        StoreFixture fixture = setUp();
        long id = fixture.push(topic, "a");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.deadLetter());

        TaskView view = fixture.view(id).orElseThrow();
        assertThat(view.state()).isEqualTo("failed");
        assertThat(view.finished()).isFalse();
        assertThat(view.error()).contains("gave up");
        assertThat(view.attempt()).isEqualTo(1);
        assertThat(fixture.counts(topic).failed()).isEqualTo(1);
        assertThat(fixture.claim(topic, node)).isEmpty();
    }

    @Test
    void suspension_delay_and_throttling_give_the_attempt_back() throws SQLException {
        StoreFixture fixture = setUp();
        long suspended = fixture.push(topic, "s");
        long delayed = fixture.push(topic, "d");
        long throttled = fixture.push(topic, "t");
        List<ClaimedTask> claimed = fixture.claim(topic, node);
        assertThat(claimed).extracting(ClaimedTask::attempt).containsOnly(1);

        fixture.inTransaction(transaction -> {
            fixture.store().complete(transaction, topic, node, List.of(
                completion(claimed.get(0), StoreFixture.suspension()),
                completion(claimed.get(1), StoreFixture.later(Duration.ofSeconds(30))),
                completion(claimed.get(2), StoreFixture.throttled(Duration.ofSeconds(30)))
            ));
            return null;
        });

        assertThat(List.of(suspended, delayed, throttled)).extracting(fixture::state).containsExactly("suspended", "scheduled", "retrying");
        assertThat(List.of(suspended, delayed, throttled)).extracting(fixture::attempt).containsOnly(0);
        for (long id : List.of(suspended, delayed, throttled)) {
            assertThat(fixture.attempts(id)).isEmpty();
        }
    }

    @Test
    void a_task_cannot_be_completed_twice_in_one_call() throws SQLException {
        StoreFixture fixture = setUp();
        fixture.push(topic, "a");
        ClaimedTask claimed = fixture.claim(topic, node).getFirst();

        assertThatThrownBy(() -> fixture.inTransaction(transaction -> {
            fixture.store().complete(transaction, topic, node, List.of(completion(claimed, StoreFixture.succeeded()), completion(claimed, StoreFixture.succeeded())));
            return null;
        })).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void expiry_requeues_other_nodes_tasks_keeping_the_attempt() throws Exception {
        StoreFixture fixture = setUp();
        long mine = fixture.push(topic, "mine");
        fixture.claim(topic, node);
        long theirs = fixture.push(topic, "theirs");
        int crashed = fixture.node("crashed");
        fixture.claim(topic, crashed);
        fixture.advance(Duration.ofHours(1));

        List<Integer> topics = fixture.requeueExpired(node, Duration.ofMinutes(10));

        assertThat(topics).containsExactly(topic);
        assertThat(fixture.state(mine)).as("a node never expires itself").isEqualTo("active");
        assertThat(fixture.state(theirs)).isEqualTo("ready");
        assertThat(fixture.attempt(theirs)).isEqualTo(1);
        assertThat(fixture.attempts(theirs)).extracting(AttemptView::result, AttemptView::nodeId).containsExactly(org.assertj.core.groups.Tuple.tuple("expired", crashed));
    }

    @Test
    void a_fresh_heartbeat_prevents_expiry() throws Exception {
        StoreFixture fixture = setUp();
        fixture.push(topic, "a");
        int other = fixture.node("node-b");
        fixture.claim(topic, other);
        fixture.advance(Duration.ofHours(1));
        fixture.heartbeat(other);

        assertThat(fixture.requeueExpired(node, Duration.ofMinutes(10))).isEmpty();
    }

    @Test
    void a_node_sees_the_claims_it_holds() throws SQLException {
        StoreFixture fixture = setUp();
        fixture.push(topic, Insertion.APPEND, task("a"), task("b"));
        int other = fixture.node("node-b");
        ClaimedTask mine = fixture.claim(topic, node, org.example.task2.Ordering.FIFO, 1).getFirst();
        fixture.claim(topic, other, org.example.task2.Ordering.FIFO, 1);

        List<TaskStore.ActiveClaim> claims = fixture.inTransaction(transaction -> fixture.store().activeClaims(transaction, node));

        assertThat(claims).singleElement().satisfies(claim -> {
            assertThat(claim.topicId()).isEqualTo(topic);
            assertThat(claim.taskId()).isEqualTo(mine.id());
            assertThat(claim.attempt()).isEqualTo(1);
            assertThat(claim.claimedAt()).isNotNull();
        });
        fixture.complete(topic, node, mine, StoreFixture.succeeded());
        List<TaskStore.ActiveClaim> after = fixture.inTransaction(transaction -> fixture.store().activeClaims(transaction, node));
        assertThat(after).isEmpty();
    }

    @Test
    void heartbeat_of_an_unknown_node_fails() throws SQLException {
        StoreFixture fixture = setUp();
        assertThatThrownBy(() -> fixture.heartbeat(9999)).isInstanceOf(IllegalStateException.class);
    }
}
