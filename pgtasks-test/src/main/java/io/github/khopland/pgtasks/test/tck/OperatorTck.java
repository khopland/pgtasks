package io.github.khopland.pgtasks.test.tck;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.admin.QueueState;
import io.github.khopland.pgtasks.admin.TaskQuery;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.spi.TaskStore.ClaimedTask;
import io.github.khopland.pgtasks.spi.TaskStore.NewTask;

import org.junit.jupiter.api.Test;

import static io.github.khopland.pgtasks.test.tck.StoreFixture.delayed;
import static io.github.khopland.pgtasks.test.tck.StoreFixture.suspended;
import static org.assertj.core.api.Assertions.assertThat;

/** TCK: an operator's search of unfinished tasks, and releasing suspended ones. */
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
    private record Tasks(long ready, long scheduled, long retrying, long active, long suspended, long deadLetter) {
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
        return new Tasks(ready, scheduled, retrying, active, suspended, deadLetter);
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
}
