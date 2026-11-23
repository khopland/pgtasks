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

/** TCK: an operator's search of unfinished tasks, cancelling them and running them now. */
public abstract class OperatorTck extends StoreTck {

    private int topic;
    private int node;

    private StoreFixture setUp() throws SQLException {
        StoreFixture fixture = fixture();
        topic = fixture.topic("topic");
        node = fixture.node("node-a");
        return fixture;
    }

    /** One task in each unfinished state, by state. */
    private record Tasks(long ready, long scheduled, long retrying, long active, long suspended, long failed) {
    }

    private Tasks everyState(StoreFixture fixture) throws SQLException {
        long retrying = fixture.push(topic, "retrying");
        long active = fixture.push(topic, "active");
        long failed = fixture.push(topic, "failed");
        List<ClaimedTask> claimed = fixture.claim(topic, node);
        fixture.complete(topic, node, byId(claimed, retrying), StoreFixture.retrying(Duration.ofHours(1)));
        fixture.complete(topic, node, byId(claimed, failed), StoreFixture.deadLetter());
        long ready = fixture.push(topic, "ready");
        long scheduled = StoreFixture.id(fixture.push(topic, Insertion.APPEND, delayed("scheduled", Duration.ofHours(1))).getFirst());
        long suspended = StoreFixture.id(fixture.push(topic, Insertion.APPEND, suspended("suspended")).getFirst());
        return new Tasks(ready, scheduled, retrying, active, suspended, failed);
    }

    private static ClaimedTask byId(List<ClaimedTask> claimed, long id) {
        return claimed.stream().filter(task -> task.id() == id).findFirst().orElseThrow();
    }

    private List<Long> search(StoreFixture fixture, TaskQuery query, int limit) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().tasks(transaction, topic, query, limit)).stream().map(TaskView::id).toList();
    }

    private List<Long> cancel(StoreFixture fixture, TaskQuery query) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().cancel(transaction, topic, query, 100));
    }

    private List<Long> runNow(StoreFixture fixture, TaskQuery query) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().runNow(transaction, topic, query, 100));
    }

    @Test
    void unfinished_tasks_in_every_state_are_listed_lowest_id_first_one_page_at_a_time() throws SQLException {
        StoreFixture fixture = setUp();
        int other = fixture.topic("other");
        long done = fixture.push(topic, "done");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.succeeded());
        Tasks tasks = everyState(fixture);
        fixture.push(other, "elsewhere");

        List<Long> all = List.of(tasks.retrying(), tasks.active(), tasks.failed(), tasks.ready(), tasks.scheduled(), tasks.suspended());
        assertThat(search(fixture, TaskQuery.ALL, 100)).as("finished tasks and other topics are left out").containsExactlyElementsOf(all)
            .doesNotContain(done);
        assertThat(search(fixture, TaskQuery.ALL, 2)).containsExactly(all.get(0), all.get(1));
        assertThat(search(fixture, TaskQuery.ALL.after(all.get(1)), 2)).containsExactly(all.get(2), all.get(3));

        TaskView view = fixture.inTransaction(transaction -> fixture.store().tasks(transaction, topic, TaskQuery.ids(List.of(tasks.failed())), 10))
            .getFirst();
        assertThat(view.topic()).isEqualTo("topic");
        assertThat(view.state()).isEqualTo("failed");
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
        assertThat(search(fixture, TaskQuery.ALL.inStates(QueueState.FAILED), 10)).containsExactly(tasks.failed());
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

    @Test
    void a_cancelled_task_leaves_the_queue_as_cancelled_in_every_state_but_active() throws SQLException {
        StoreFixture fixture = setUp();
        Tasks tasks = everyState(fixture);

        List<Long> cancelled = cancel(fixture, TaskQuery.ALL);

        assertThat(cancelled).containsExactlyInAnyOrder(tasks.ready(), tasks.scheduled(), tasks.retrying(), tasks.suspended(), tasks.failed()).isSorted();
        for (long id : cancelled) {
            TaskView view = fixture.view(id).orElseThrow();
            assertThat(view.finished()).isTrue();
            assertThat(view.state()).isEqualTo("cancelled");
            assertThat(view.completedAt()).isNotNull();
        }
        assertThat(fixture.view(tasks.failed()).orElseThrow().attempt()).as("the attempts it took").isEqualTo(1);
        assertThat(fixture.view(tasks.failed()).orElseThrow().error()).as("its last error stays").contains("gave up");
        assertThat(fixture.state(tasks.active())).as("a node runs it").isEqualTo("active");
        assertThat(fixture.counts(topic).total()).isOne();
        assertThat(cancel(fixture, TaskQuery.ALL)).as("cancelling twice does nothing").isEmpty();
    }

    @Test
    void an_active_task_is_not_cancelled_and_its_completion_still_applies() throws SQLException {
        StoreFixture fixture = setUp();
        long id = fixture.push(topic, "a");
        ClaimedTask claimed = fixture.claim(topic, node).getFirst();

        assertThat(cancel(fixture, TaskQuery.ids(List.of(id)))).isEmpty();
        fixture.complete(topic, node, claimed, StoreFixture.succeeded());

        assertThat(fixture.state(id)).isEqualTo("succeeded");
    }

    @Test
    void cancelling_by_a_query_takes_only_what_matches_and_the_lowest_ids_first() throws SQLException {
        StoreFixture fixture = setUp();
        long first = fixture.push(topic, "a");
        long other = fixture.push(topic, "b");
        long second = fixture.push(topic, "a");
        long third = fixture.push(topic, "a");

        List<Long> cancelled = fixture.inTransaction(transaction -> fixture.store().cancel(transaction, topic, TaskQuery.ALL.withIdentifier("a"), 2));
        assertThat(cancelled).containsExactly(first, second);
        assertThat(cancel(fixture, TaskQuery.ALL.withIdentifier("a").after(third))).isEmpty();
        assertThat(cancel(fixture, TaskQuery.ids(List.of(third, 999_999L)))).containsExactly(third);
        assertThat(fixture.state(other)).isEqualTo("ready");
    }

    @Test
    void cancelling_a_task_lets_the_tasks_it_held_back_run() throws SQLException {
        StoreFixture fixture = setUp();
        long retrying = fixture.push(topic, "a");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.retrying(Duration.ofHours(1)));
        long next = fixture.push(topic, "a");
        assertThat(fixture.claim(topic, node, Ordering.SINGULAR_BY_IDENTIFIER, 10)).as("held back by the retrying task").isEmpty();

        cancel(fixture, TaskQuery.ids(List.of(retrying)));

        assertThat(fixture.claim(topic, node, Ordering.SINGULAR_BY_IDENTIFIER, 10)).extracting(ClaimedTask::id).containsExactly(next);
    }

    @Test
    void running_now_makes_delayed_and_retrying_tasks_ready_and_keeps_their_attempts() throws SQLException {
        StoreFixture fixture = setUp();
        Tasks tasks = everyState(fixture);

        assertThat(runNow(fixture, TaskQuery.ALL)).containsExactly(tasks.retrying(), tasks.scheduled());

        assertThat(fixture.state(tasks.retrying())).isEqualTo("ready");
        assertThat(fixture.state(tasks.scheduled())).isEqualTo("ready");
        TaskView retried = fixture.view(tasks.retrying()).orElseThrow();
        assertThat(retried.runAt()).isNull();
        assertThat(retried.attempt()).isEqualTo(1);
        assertThat(retried.failures()).as("its failures stay, so a further one backs off as before").isOne();
        assertThat(fixture.state(tasks.suspended())).isEqualTo("suspended");
        assertThat(fixture.state(tasks.failed())).isEqualTo("failed");
        assertThat(fixture.state(tasks.active())).isEqualTo("active");

        List<ClaimedTask> claimed = fixture.claim(topic, node);
        assertThat(claimed).extracting(ClaimedTask::id).contains(tasks.retrying(), tasks.scheduled());
        assertThat(byId(claimed, tasks.retrying()).attempt()).as("the next attempt").isEqualTo(2);
        assertThat(runNow(fixture, TaskQuery.ALL)).as("nothing left to run now").isEmpty();
    }

    @Test
    void running_now_by_id_skips_what_does_not_wait_for_a_time() throws SQLException {
        StoreFixture fixture = setUp();
        Tasks tasks = everyState(fixture);

        assertThat(runNow(fixture, TaskQuery.ids(List.of(tasks.scheduled(), tasks.ready(), tasks.suspended(), 999_999L))))
            .containsExactly(tasks.scheduled());
        assertThat(fixture.state(tasks.retrying())).isEqualTo("retrying");
    }
}
