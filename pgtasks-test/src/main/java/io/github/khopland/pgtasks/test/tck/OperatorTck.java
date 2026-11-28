package io.github.khopland.pgtasks.test.tck;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.admin.QueueState;
import io.github.khopland.pgtasks.admin.TaskQuery;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.spi.TaskStore.ClaimedTask;
import io.github.khopland.pgtasks.spi.TaskStore.NewTask;

import org.junit.jupiter.api.Test;

import static io.github.khopland.pgtasks.test.tck.StoreFixture.delayed;
import static io.github.khopland.pgtasks.test.tck.StoreFixture.suspended;
import static org.assertj.core.api.Assertions.assertThat;

/** TCK: an operator's search of unfinished tasks, releasing suspended ones, running waiting ones now, and cancelling. */
public abstract class OperatorTck extends StoreTck {

    private int topic;
    private int node;

    private StoreFixture setUp() throws SQLException {
        StoreFixture fixture = fixture();
        topic = fixture.topic("topic");
        node = fixture.node("node-a");
        return fixture;
    }

    /** One task in each unfinished state, by state, and the claim of the active one. */
    private record Tasks(long ready, long scheduled, long retrying, long active, long suspended, long deadLetter, ClaimedTask claim) {
    }

    private Tasks everyState(StoreFixture fixture) throws SQLException {
        long retrying = fixture.push(topic, "retrying");
        long active = fixture.push(topic, "active");
        long deadLetter = fixture.push(topic, "dead");
        List<ClaimedTask> claimed = fixture.claim(topic, node);
        fixture.complete(topic, node, byId(claimed, retrying), StoreFixture.retrying(Duration.ofHours(1)));
        fixture.complete(topic, node, byId(claimed, deadLetter), StoreFixture.deadLetter());
        long ready = fixture.push(topic, "ready");
        long scheduled = StoreFixture.id(fixture.push(topic, Insertion.APPEND, delayed("scheduled", Duration.ofHours(1))).getFirst());
        long suspended = StoreFixture.id(fixture.push(topic, Insertion.APPEND, suspended("suspended")).getFirst());
        return new Tasks(ready, scheduled, retrying, active, suspended, deadLetter, byId(claimed, active));
    }

    private static ClaimedTask byId(List<ClaimedTask> claimed, long id) {
        return claimed.stream().filter(task -> task.id() == id).findFirst().orElseThrow();
    }

    private List<Long> search(StoreFixture fixture, TaskQuery query, int limit) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().tasks(transaction, topic, query, limit)).stream().map(TaskView::id).toList();
    }

    @Test
    void unfinished_tasks_in_every_state_are_listed_lowest_id_first_one_page_at_a_time() throws SQLException {
        StoreFixture fixture = setUp();
        int other = fixture.topic("other");
        long done = fixture.push(topic, "done");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.succeeded());
        Tasks tasks = everyState(fixture);
        fixture.push(other, "elsewhere");

        List<Long> all = List.of(tasks.retrying(), tasks.active(), tasks.deadLetter(), tasks.ready(), tasks.scheduled(), tasks.suspended());
        assertThat(search(fixture, TaskQuery.ALL, 100)).as("finished tasks and other topics are left out").containsExactlyElementsOf(all)
            .doesNotContain(done);
        assertThat(search(fixture, TaskQuery.ALL, 2)).containsExactly(all.get(0), all.get(1));
        assertThat(search(fixture, TaskQuery.ALL.after(all.get(1)), 2)).containsExactly(all.get(2), all.get(3));

        TaskView view = fixture.inTransaction(transaction -> fixture.store().tasks(transaction, topic, TaskQuery.ids(List.of(tasks.deadLetter())), 10))
            .getFirst();
        assertThat(view.topic()).isEqualTo("topic");
        assertThat(view.state()).isEqualTo("dead_letter");
        assertThat(view.finished()).isFalse();
        assertThat(view.error()).contains("gave up");
        assertThat(view.attempt()).isEqualTo(1);
    }

    @Test
    void unfinished_tasks_are_found_by_identifier_reference_state_header_and_when_they_were_pushed() throws SQLException {
        StoreFixture fixture = setUp();
        Tasks tasks = everyState(fixture);
        long tagged = StoreFixture.id(fixture.push(topic, Insertion.APPEND, new NewTask("person-7", null, null, "case-42", null, false, 0, null, null, null,
            Map.of("tenant", "north"))).getFirst());
        Instant now = fixture.store().clock().instant();

        assertThat(search(fixture, TaskQuery.ALL.withIdentifier("person-7"), 10)).containsExactly(tagged);
        assertThat(search(fixture, TaskQuery.ALL.withReference("case-42"), 10)).containsExactly(tagged);
        assertThat(search(fixture, TaskQuery.ALL.withHeader("tenant", "north"), 10)).containsExactly(tagged);
        assertThat(search(fixture, TaskQuery.ALL.withHeader("tenant", "south"), 10)).isEmpty();
        assertThat(search(fixture, TaskQuery.ALL.inStates(QueueState.DEAD_LETTER), 10)).containsExactly(tasks.deadLetter());
        assertThat(search(fixture, TaskQuery.ALL.inStates(QueueState.SCHEDULED, QueueState.RETRYING), 10))
            .containsExactly(tasks.retrying(), tasks.scheduled());
        assertThat(search(fixture, TaskQuery.ALL.inStates(QueueState.READY), 10)).containsExactly(tasks.ready(), tagged);
        assertThat(search(fixture, TaskQuery.ALL.inStates(QueueState.ACTIVE).withIdentifier("active"), 10)).containsExactly(tasks.active());
        assertThat(search(fixture, TaskQuery.ALL.createdBetween(now.plus(Duration.ofHours(1)), null), 10)).isEmpty();
        assertThat(search(fixture, TaskQuery.ALL.createdBetween(now.minus(Duration.ofHours(1)), now.plus(Duration.ofHours(1))), 10)).hasSize(7);
        assertThat(search(fixture, TaskQuery.ALL.createdBetween(null, now.minus(Duration.ofHours(1))), 10)).isEmpty();
        assertThat(search(fixture, TaskQuery.ALL.withIdentifier("person-7").inStates(QueueState.SUSPENDED), 10)).as("every condition holds").isEmpty();
        assertThat(search(fixture, TaskQuery.ids(List.of(tasks.ready(), 999_999L)), 10)).containsExactly(tasks.ready());
    }

    private List<Long> release(StoreFixture fixture, TaskQuery query, int limit) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().releaseSuspended(transaction, topic, query, limit));
    }

    @Test
    void releasing_makes_suspended_tasks_ready_and_leaves_every_other_state_alone() throws SQLException {
        StoreFixture fixture = setUp();
        long held = fixture.push(topic, "held");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.suspension());
        Tasks tasks = everyState(fixture);

        assertThat(release(fixture, TaskQuery.ALL, 100)).containsExactly(held, tasks.suspended());

        assertThat(List.of(held, tasks.suspended())).extracting(fixture::state).containsOnly("ready");
        assertThat(fixture.attempt(held)).as("a suspension gave its attempt back, and a release keeps that").isZero();
        assertThat(List.of(tasks.ready(), tasks.scheduled(), tasks.retrying(), tasks.active(), tasks.deadLetter()))
            .extracting(fixture::state).containsExactly("ready", "scheduled", "retrying", "active", "dead_letter");
        assertThat(release(fixture, TaskQuery.ALL, 100)).as("nothing is suspended any more").isEmpty();
        assertThat(StoreFixture.ids(fixture.claim(topic, node))).contains(held, tasks.suspended());
    }

    @Test
    void releasing_takes_only_suspended_tasks_that_match_the_query_one_page_at_a_time() throws SQLException {
        StoreFixture fixture = setUp();
        long first = StoreFixture.id(fixture.push(topic, Insertion.APPEND, suspended("order-1")).getFirst());
        long second = StoreFixture.id(fixture.push(topic, Insertion.APPEND, suspended("order-2")).getFirst());
        long third = StoreFixture.id(fixture.push(topic, Insertion.APPEND, suspended("order-3")).getFirst());
        long ready = fixture.push(topic, "order-2");

        assertThat(release(fixture, TaskQuery.ids(List.of(ready, 999_999L)), 10)).as("not suspended, or not there").isEmpty();
        assertThat(release(fixture, TaskQuery.ALL.withIdentifier("order-2"), 10)).containsExactly(second);
        assertThat(release(fixture, TaskQuery.ALL, 1)).containsExactly(first);
        assertThat(release(fixture, TaskQuery.ALL.after(third), 10)).isEmpty();
        assertThat(release(fixture, TaskQuery.ids(List.of(third)), 10)).containsExactly(third);
        assertThat(fixture.state(ready)).isEqualTo("ready");
    }

    private List<Long> runNow(StoreFixture fixture, TaskQuery query, int limit) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().runNow(transaction, topic, query, limit));
    }

    @Test
    void running_now_makes_delayed_and_retrying_tasks_ready_and_keeps_their_attempts() throws SQLException {
        StoreFixture fixture = setUp();
        int other = fixture.topic("other");
        long later = fixture.push(topic, "later");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.later(Duration.ofHours(1)));
        Tasks tasks = everyState(fixture);
        long elsewhere = StoreFixture.id(fixture.push(other, Insertion.APPEND, delayed("elsewhere", Duration.ofHours(1))).getFirst());

        assertThat(runNow(fixture, TaskQuery.ALL, 100)).containsExactly(later, tasks.retrying(), tasks.scheduled());

        assertThat(List.of(later, tasks.retrying(), tasks.scheduled())).extracting(fixture::state).containsOnly("ready");
        TaskView retried = fixture.view(tasks.retrying()).orElseThrow();
        assertThat(retried.runAt()).isNull();
        assertThat(retried.attempt()).isEqualTo(1);
        assertThat(retried.failures()).as("its failures stay, so a further one backs off from where it was").isOne();
        assertThat(retried.error()).as("the failure it had").isNotNull();
        assertThat(fixture.attempt(later)).as("later() gave its attempt back, and running it now keeps that").isZero();
        assertThat(List.of(tasks.ready(), tasks.active(), tasks.suspended(), tasks.deadLetter(), elsewhere)).extracting(fixture::state)
            .containsExactly("ready", "active", "suspended", "dead_letter", "scheduled");
        assertThat(runNow(fixture, TaskQuery.ALL, 100)).as("nothing waits for a time any more").isEmpty();

        List<ClaimedTask> claimed = fixture.claim(topic, node);
        assertThat(StoreFixture.ids(claimed)).contains(later, tasks.retrying(), tasks.scheduled());
        assertThat(byId(claimed, tasks.retrying()).attempt()).as("the next attempt").isEqualTo(2);
    }

    @Test
    void running_now_takes_only_waiting_tasks_that_match_the_query_one_page_at_a_time() throws SQLException {
        StoreFixture fixture = setUp();
        Tasks tasks = everyState(fixture);
        long first = StoreFixture.id(fixture.push(topic, Insertion.APPEND, delayed("order-1", Duration.ofHours(1))).getFirst());
        long second = StoreFixture.id(fixture.push(topic, Insertion.APPEND, delayed("order-2", Duration.ofHours(1))).getFirst());
        long third = StoreFixture.id(fixture.push(topic, Insertion.APPEND, delayed("order-3", Duration.ofHours(1))).getFirst());

        assertThat(runNow(fixture, TaskQuery.ids(List.of(tasks.ready(), tasks.active(), tasks.suspended(), tasks.deadLetter(), 999_999L)), 10))
            .as("not waiting for a time, or not there").isEmpty();
        assertThat(runNow(fixture, TaskQuery.ALL.inStates(QueueState.READY, QueueState.SUSPENDED), 10)).as("asks for no waiting state").isEmpty();
        assertThat(runNow(fixture, TaskQuery.ALL.withIdentifier("order-2"), 10)).containsExactly(second);
        assertThat(runNow(fixture, TaskQuery.ALL.inStates(QueueState.RETRYING, QueueState.READY), 10)).containsExactly(tasks.retrying());
        assertThat(runNow(fixture, TaskQuery.ALL.after(tasks.scheduled()), 1)).containsExactly(first);
        assertThat(runNow(fixture, TaskQuery.ids(List.of(third, tasks.scheduled())), 10)).containsExactly(tasks.scheduled(), third);
        assertThat(List.of(first, second, third)).extracting(fixture::state).containsOnly("ready");
    }

    @Test
    void running_a_retrying_task_now_lets_it_and_the_tasks_it_held_back_run_in_order() throws SQLException {
        StoreFixture fixture = setUp();
        long held = fixture.push(topic, "order-1");
        long next = fixture.push(topic, "order-1");
        fixture.complete(topic, node, fixture.claim(topic, node, Ordering.SINGULAR_BY_IDENTIFIER, 1).getFirst(),
            StoreFixture.retrying(Duration.ofHours(1)));
        assertThat(fixture.claim(topic, node, Ordering.SINGULAR_BY_IDENTIFIER, 10)).as("held back by the retrying task").isEmpty();

        assertThat(runNow(fixture, TaskQuery.ids(List.of(held)), 10)).containsExactly(held);

        ClaimedTask retried = fixture.claim(topic, node, Ordering.SINGULAR_BY_IDENTIFIER, 10).getFirst();
        assertThat(retried.id()).as("the retried task first, the one it held back after it").isEqualTo(held);
        assertThat(fixture.claim(topic, node, Ordering.SINGULAR_BY_IDENTIFIER, 10)).as("still one at a time").isEmpty();
        fixture.complete(topic, node, retried, StoreFixture.succeeded());
        assertThat(fixture.claim(topic, node, Ordering.SINGULAR_BY_IDENTIFIER, 10)).extracting(ClaimedTask::id).containsExactly(next);
    }

    private List<Long> cancel(StoreFixture fixture, TaskQuery query, int limit) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().cancel(transaction, topic, query, limit));
    }

    @Test
    void cancelling_finishes_every_unfinished_state_but_active_as_cancelled() throws SQLException {
        StoreFixture fixture = setUp();
        int other = fixture.topic("other");
        long done = fixture.push(topic, "done");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.succeeded());
        Tasks tasks = everyState(fixture);
        long elsewhere = fixture.push(other, "elsewhere");

        assertThat(cancel(fixture, TaskQuery.ALL, 100))
            .containsExactly(tasks.retrying(), tasks.deadLetter(), tasks.ready(), tasks.scheduled(), tasks.suspended());

        List<Long> cancelled = List.of(tasks.retrying(), tasks.deadLetter(), tasks.ready(), tasks.scheduled(), tasks.suspended());
        assertThat(cancelled).extracting(fixture::state).containsOnly("cancelled");
        TaskView retried = fixture.view(tasks.retrying()).orElseThrow();
        assertThat(retried.finished()).isTrue();
        assertThat(retried.completedAt()).isNotNull();
        assertThat(retried.attempt()).as("the attempts it took").isEqualTo(1);
        assertThat(retried.error()).as("the failure it had").isNotNull();
        assertThat(fixture.state(done)).isEqualTo("succeeded");
        assertThat(fixture.state(elsewhere)).isEqualTo("ready");
        assertThat(fixture.state(tasks.active())).as("a node runs it").isEqualTo("active");
        assertThat(fixture.counts(topic).total()).isEqualTo(1);
        assertThat(cancel(fixture, TaskQuery.ALL, 100)).as("only the active task is left").isEmpty();
        assertThat(fixture.claim(topic, node)).as("nothing cancelled runs").isEmpty();

        // The node that runs the active task still completes it.
        fixture.complete(topic, node, tasks.claim(), StoreFixture.succeeded());
        assertThat(fixture.state(tasks.active())).isEqualTo("succeeded");
    }

    @Test
    void cancelling_takes_only_tasks_that_match_the_query_one_page_at_a_time() throws SQLException {
        StoreFixture fixture = setUp();
        Tasks tasks = everyState(fixture);
        long first = fixture.push(topic, "order-1");
        long second = fixture.push(topic, "order-2");
        long third = fixture.push(topic, "order-3");

        assertThat(cancel(fixture, TaskQuery.ids(List.of(tasks.active(), 999_999L)), 10)).as("active, or not there").isEmpty();
        assertThat(cancel(fixture, TaskQuery.ALL.inStates(QueueState.ACTIVE), 10)).as("an active task never is").isEmpty();
        assertThat(cancel(fixture, TaskQuery.ALL.withIdentifier("order-2"), 10)).containsExactly(second);
        assertThat(cancel(fixture, TaskQuery.ALL.inStates(QueueState.SUSPENDED, QueueState.DEAD_LETTER), 10))
            .containsExactly(tasks.deadLetter(), tasks.suspended());
        assertThat(cancel(fixture, TaskQuery.ALL.after(tasks.suspended()), 1)).containsExactly(first);
        assertThat(cancel(fixture, TaskQuery.ids(List.of(third, tasks.ready())), 10)).containsExactly(tasks.ready(), third);
        assertThat(List.of(tasks.retrying(), tasks.scheduled(), tasks.active())).extracting(fixture::state)
            .containsExactly("retrying", "scheduled", "active");
    }

    @Test
    void cancelling_lets_the_tasks_it_held_back_run() throws SQLException {
        StoreFixture fixture = setUp();
        long held = fixture.push(topic, "order-1");
        long next = fixture.push(topic, "order-1");
        fixture.complete(topic, node, fixture.claim(topic, node, Ordering.SINGULAR_BY_IDENTIFIER, 1).getFirst(),
            StoreFixture.retrying(Duration.ofHours(1)));
        assertThat(fixture.claim(topic, node, Ordering.SINGULAR_BY_IDENTIFIER, 10)).as("held back by the retrying task").isEmpty();

        assertThat(cancel(fixture, TaskQuery.ids(List.of(held)), 10)).containsExactly(held);

        assertThat(fixture.claim(topic, node, Ordering.SINGULAR_BY_IDENTIFIER, 10)).extracting(ClaimedTask::id).containsExactly(next);
    }
}
