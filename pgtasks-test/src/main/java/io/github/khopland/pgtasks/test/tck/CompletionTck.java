package io.github.khopland.pgtasks.test.tck;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.admin.AttemptView;
import io.github.khopland.pgtasks.admin.FailureGroup;
import io.github.khopland.pgtasks.admin.QueueCounts;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.spi.StaleClaimException;
import io.github.khopland.pgtasks.spi.TaskStore;
import io.github.khopland.pgtasks.spi.TaskStore.ClaimedTask;

import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;

import static io.github.khopland.pgtasks.test.tck.StoreFixture.completion;
import static io.github.khopland.pgtasks.test.tck.StoreFixture.task;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    /** Fails the task's current claim with a retry due at once, and makes it ready again. */
    private ClaimedTask failAndRetry(StoreFixture fixture, long id) throws Exception {
        ClaimedTask claimed = fixture.claim(topic, node).stream().filter(task -> task.id() == id).findFirst().orElseThrow();
        fixture.complete(topic, node, claimed, StoreFixture.retrying(Duration.ZERO));
        fixture.promoteDue();
        return claimed;
    }

    @Test
    void an_uncounted_failure_gives_the_attempt_back_but_counts_as_a_failure_and_keeps_its_error() throws Exception {
        StoreFixture fixture = setUp();
        long id = fixture.push(topic, "a");
        ClaimedTask first = fixture.claim(topic, node).getFirst();
        assertThat(first.failures()).isZero();
        assertThat(first.firstFailedAt()).isNull();

        fixture.complete(topic, node, first, new TaskStore.Resolution.Waiting(TaskStore.WaitState.RETRYING, Duration.ZERO, false,
            "{\"message\":\"downstream down\"}"));

        TaskView view = fixture.view(id).orElseThrow();
        assertThat(view.state()).isEqualTo("retrying");
        assertThat(view.attempt()).as("the attempt is given back").isZero();
        assertThat(view.failures()).isEqualTo(1);
        assertThat(view.firstFailedAt()).isNotNull();
        assertThat(view.error()).contains("downstream down");
        assertThat(fixture.attempts(id)).as("uncounted failures aren't logged").isEmpty();

        fixture.promoteDue();
        ClaimedTask again = fixture.claim(topic, node).getFirst();
        assertThat(again.attempt()).isEqualTo(1);
        assertThat(again.failures()).isEqualTo(1);
        assertThat(again.firstFailedAt()).as("a time budget counts from it").isEqualTo(view.firstFailedAt());
    }

    @Test
    void a_counted_failure_keeps_its_error_on_the_task_while_it_retries() throws Exception {
        StoreFixture fixture = setUp();
        long id = fixture.push(topic, "a");
        Instant firstFailed;
        failAndRetry(fixture, id);
        firstFailed = fixture.view(id).orElseThrow().firstFailedAt();
        failAndRetry(fixture, id);

        TaskView view = fixture.view(id).orElseThrow();
        assertThat(view.error()).contains("boom");
        assertThat(view.failures()).isEqualTo(2);
        assertThat(view.firstFailedAt()).as("when the first of the run failed").isEqualTo(firstFailed);
    }

    @Test
    void failing_tasks_are_counted_and_grouped_by_error() throws Exception {
        StoreFixture fixture = setUp();
        fixture.push(topic, Insertion.APPEND, task("a"), task("b"), task("c"), task("fine"));
        String refused = "{\"message\":\"refused\",\"type\":\"java.net.ConnectException\"}";
        for (ClaimedTask claimed : fixture.claim(topic, node, Ordering.FIFO, 10)) {
            TaskStore.Resolution resolution = switch (claimed.identifier()) {
                case "a", "b" -> new TaskStore.Resolution.Waiting(TaskStore.WaitState.RETRYING, Duration.ofHours(1), false, refused);
                case "c" -> StoreFixture.deadLetter();
                default -> StoreFixture.succeeded();
            };
            fixture.complete(topic, node, claimed, resolution);
        }

        QueueCounts counts = fixture.counts(topic);
        assertThat(counts.failing()).as("retrying and dead-lettered alike").isEqualTo(3);
        assertThat(counts.oldestFailingAt()).isNotNull();
        List<FailureGroup> groups = fixture.inTransaction(transaction -> fixture.store().failing(transaction, topic, 10));
        assertThat(groups).extracting(FailureGroup::error, FailureGroup::tasks).containsExactly(
            Tuple.tuple("java.net.ConnectException", 2L),
            Tuple.tuple("gave up", 1L));
        assertThat(groups.getFirst().exampleMessage()).isEqualTo("refused");
        assertThat(groups.getFirst().oldestFailedAt()).isNotNull();
    }

    @Test
    void the_attempt_log_keeps_a_tasks_first_attempt_and_its_last_ten() throws Exception {
        StoreFixture fixture = setUp();
        long id = fixture.push(topic, "a");
        for (int attempt = 1; attempt <= 15; attempt++) {
            failAndRetry(fixture, id);
        }

        assertThat(fixture.attempts(id)).extracting(AttemptView::attempt).containsExactly(1, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    }

    @Test
    void a_tasks_attempts_go_when_retention_or_a_delete_push_removes_it() throws Exception {
        StoreFixture fixture = setUp();
        long purged = fixture.push(topic, "a");
        failAndRetry(fixture, purged);
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.succeeded());
        long replaced = fixture.push(topic, "b");
        failAndRetry(fixture, replaced);
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.succeeded());
        assertThat(fixture.attempts(purged)).hasSize(1);
        assertThat(fixture.attempts(replaced)).hasSize(1);

        int deleted = fixture.inTransaction(transaction -> fixture.store().purge(transaction, topic, io.github.khopland.pgtasks.Outcome.SUCCEEDED,
            fixture.store().clock().instant().plusSeconds(60), 1));
        assertThat(deleted).isEqualTo(1);
        assertThat(fixture.attempts(purged)).isEmpty();

        fixture.push(topic, Insertion.DELETE, StoreFixture.task("b"));
        assertThat(fixture.view(replaced)).isEmpty();
        assertThat(fixture.attempts(replaced)).isEmpty();
    }

    @Test
    void promotion_respects_its_limit() throws Exception {
        StoreFixture fixture = setUp();
        fixture.push(topic, Insertion.APPEND, StoreFixture.delayed("a", Duration.ofSeconds(10)), StoreFixture.delayed("b", Duration.ofSeconds(10)),
            StoreFixture.delayed("c", Duration.ofSeconds(10)), StoreFixture.delayed("d", Duration.ofSeconds(10)), StoreFixture.delayed("e", Duration.ofSeconds(10)));
        fixture.advance(Duration.ofSeconds(20));

        // The count, not the topics, tells the caller whether more are due: one topic, two tasks.
        TaskStore.Promoted promoted = fixture.inTransaction(transaction -> fixture.store().promoteDue(transaction, 2));
        assertThat(promoted.tasks()).isEqualTo(2);
        assertThat(promoted.topics()).containsExactly(topic);
        assertThat(fixture.counts(topic).ready()).isEqualTo(2);
        assertThat(fixture.counts(topic).due()).isEqualTo(3);

        TaskStore.Promoted rest = fixture.inTransaction(transaction -> fixture.store().promoteDue(transaction, 2));
        assertThat(rest.tasks()).isEqualTo(2);
        TaskStore.Promoted last = fixture.inTransaction(transaction -> fixture.store().promoteDue(transaction, 2));
        assertThat(last.tasks()).isEqualTo(1);
        assertThat(fixture.counts(topic).ready()).isEqualTo(5);
        TaskStore.Promoted none = fixture.inTransaction(transaction -> fixture.store().promoteDue(transaction, 2));
        assertThat(none).isEqualTo(TaskStore.Promoted.NONE);
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
        assertThat(fixture.attempts(theirs)).extracting(AttemptView::result, AttemptView::nodeId).containsExactly(Tuple.tuple("expired", crashed));
    }

    @Test
    void each_expiry_backs_off_longer_and_the_last_one_dead_letters_the_task() throws Exception {
        StoreFixture fixture = setUp();
        long id = fixture.push(topic, "a");
        TaskStore.Expiries expiries = new TaskStore.Expiries(3, Duration.ofSeconds(30));
        for (int expiry = 1; expiry <= 3; expiry++) {
            int crashed = fixture.node("crashed-" + expiry);
            fixture.claim(topic, crashed);
            fixture.advance(Duration.ofHours(1));
            fixture.heartbeat(node);
            Instant now = fixture.store().clock().instant();
            assertThat(fixture.requeueExpired(node, Duration.ofMinutes(10), expiries)).containsExactly(topic);

            TaskView view = fixture.view(id).orElseThrow();
            assertThat(view.failures()).isEqualTo(expiry);
            assertThat(view.error()).contains("Expired");
            if (expiry < 3) {
                assertThat(view.state()).isEqualTo("retrying");
                Duration backoff = Duration.ofSeconds(30L << (expiry - 1));
                assertThat(view.runAt()).isBetween(now.plus(backoff).minusSeconds(5), now.plus(backoff).plusSeconds(5));
                fixture.advance(backoff);
                fixture.promoteDue();
            } else {
                assertThat(view.state()).as("dead letter at the third expiry").isEqualTo("failed");
                assertThat(view.error()).contains("Expired 3 times");
            }
        }
        assertThat(fixture.attempts(id)).extracting(AttemptView::result).containsOnly("expired");
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
        ClaimedTask mine = fixture.claim(topic, node, Ordering.FIFO, 1).getFirst();
        fixture.claim(topic, other, Ordering.FIFO, 1);

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

    /** Review 2026-11-23: the orphan sweep compared database claim times with a cutoff from the JVM's clock. */
    @Test
    void a_node_sees_only_the_claims_older_than_a_grace_by_the_stores_clock() throws Exception {
        StoreFixture fixture = setUp();
        fixture.push(topic, Insertion.APPEND, task("a"), task("b"));
        ClaimedTask old = fixture.claim(topic, node, Ordering.FIFO, 1).getFirst();
        // Time passes by either store's clock: the in-memory store's is moved, the database's runs.
        fixture.advance(Duration.ofMillis(200));
        Thread.sleep(200);
        ClaimedTask fresh = fixture.claim(topic, node, Ordering.FIFO, 1).getFirst();

        List<TaskStore.ActiveClaim> settled = fixture.inTransaction(transaction -> fixture.store().activeClaims(transaction, node, Duration.ofMillis(100)));
        List<TaskStore.ActiveClaim> all = fixture.inTransaction(transaction -> fixture.store().activeClaims(transaction, node, Duration.ZERO));
        List<TaskStore.ActiveClaim> none = fixture.inTransaction(transaction -> fixture.store().activeClaims(transaction, node, Duration.ofHours(1)));

        assertThat(settled).extracting(TaskStore.ActiveClaim::taskId).containsExactly(old.id());
        assertThat(all).extracting(TaskStore.ActiveClaim::taskId).containsExactlyInAnyOrder(old.id(), fresh.id());
        assertThat(none).isEmpty();
    }

    @Test
    void heartbeat_of_an_unknown_node_says_so() throws SQLException {
        StoreFixture fixture = setUp();
        assertThat(fixture.heartbeat(9999)).isFalse();
        assertThat(fixture.heartbeat(node)).isTrue();
    }
}
