package io.github.khopland.pgtasks.test.tck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static io.github.khopland.pgtasks.test.tck.StoreFixture.deduplicated;
import static io.github.khopland.pgtasks.test.tck.StoreFixture.delayed;
import static io.github.khopland.pgtasks.test.tck.StoreFixture.suspended;
import static io.github.khopland.pgtasks.test.tck.StoreFixture.task;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.admin.QueueCounts;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;
import org.junit.jupiter.api.Test;

/** TCK: pushes and the four insertion policies. */
public abstract class PushTck extends StoreTck {

    private int topic, other, node;

    private StoreFixture setUp() throws SQLException {
        StoreFixture fixture = fixture();
        topic = fixture.topic("topic");
        other = fixture.topic("other");
        node = fixture.node("node-a");
        return fixture;
    }

    @Test
    void append_stores_and_queues_in_order() throws SQLException {
        StoreFixture fixture = setUp();
        List<Pushed> pushed = fixture.push(topic, Insertion.APPEND, task("a"), task("b"), task("a"));

        assertThat(pushed).extracting(Pushed::status).containsOnly(Pushed.Status.QUEUED);
        assertThat(pushed).extracting(Pushed::id).isSorted().doesNotHaveDuplicates();
        for (Pushed each : pushed) {
            TaskView view = fixture.view(each.id()).orElseThrow();
            assertThat(view.topic()).isEqualTo("topic");
            assertThat(view.identifier()).isEqualTo(each.identifier());
            assertThat(view.state()).isEqualTo("ready");
            assertThat(view.payload()).isEqualTo("payload-" + each.identifier());
            assertThat(view.finished()).isFalse();
            assertThat(view.createdAt()).isNotNull();
        }
    }

    @Test
    void delayed_and_suspended_pushes_wait() throws SQLException {
        StoreFixture fixture = setUp();
        Instant before = fixture.store().clock().instant();
        List<Pushed> pushed = fixture.push(topic, Insertion.APPEND, delayed("a", Duration.ofMinutes(5)), suspended("b"), delayed("c", Duration.ZERO));

        assertThat(pushed).extracting(Pushed::id).extracting(fixture::state).containsExactly("scheduled", "suspended", "ready");
        assertThat(fixture.view(pushed.getFirst().id()).orElseThrow().runAt())
            .isBetween(before.plus(Duration.ofMinutes(4)), before.plus(Duration.ofMinutes(6)));
        QueueCounts counts = fixture.counts(topic);
        assertThat(counts).isEqualTo(new QueueCounts(1, 0, 1, 0, 1, 0, 0, counts.nextRunAt(), counts.oldestReadyAt(), 0, null));
        assertThat(counts.nextRunAt()).isEqualTo(fixture.view(pushed.getFirst().id()).orElseThrow().runAt());
        assertThat(counts.oldestReadyAt()).isEqualTo(fixture.view(pushed.get(2).id()).orElseThrow().createdAt());
    }

    @Test
    void push_to_an_unknown_topic_fails() throws SQLException {
        StoreFixture fixture = setUp();
        assertThatThrownBy(() -> fixture.push(9999, Insertion.APPEND, task("a"))).isInstanceOf(SQLException.class);
    }

    @Test
    void supersede_makes_waiting_tasks_redundant_but_not_running_or_parked_ones() throws SQLException {
        StoreFixture fixture = setUp();
        long running = fixture.push(topic, "a");
        fixture.claim(topic, node);
        long ready = fixture.push(topic, "a");
        long delayedTask = fixture.push(topic, Insertion.APPEND, delayed("a", Duration.ofHours(1))).getFirst().id();
        long parked = fixture.push(topic, Insertion.APPEND, suspended("a")).getFirst().id();
        long otherIdentifier = fixture.push(topic, "b");
        long otherTopic = fixture.push(other, "a");

        List<Pushed> pushed = fixture.push(topic, Insertion.SUPERSEDE, task("a"), task("a"));

        assertThat(pushed).extracting(Pushed::status).containsExactly(Pushed.Status.REDUNDANT, Pushed.Status.QUEUED);
        assertThat(fixture.state(running)).isEqualTo("active");
        assertThat(fixture.state(ready)).isEqualTo("redundant");
        assertThat(fixture.state(delayedTask)).isEqualTo("redundant");
        assertThat(fixture.state(parked)).isEqualTo("suspended");
        assertThat(fixture.state(otherIdentifier)).isEqualTo("ready");
        assertThat(fixture.state(otherTopic)).isEqualTo("ready");
        assertThat(fixture.state(pushed.get(0).id())).isEqualTo("redundant");
        assertThat(fixture.state(pushed.get(1).id())).isEqualTo("ready");
        assertThat(fixture.view(ready).orElseThrow().finished()).isTrue();
    }

    @Test
    void supersede_covers_a_task_that_is_backing_off() throws SQLException {
        StoreFixture fixture = setUp();
        fixture.push(topic, "a");
        var claimed = fixture.claim(topic, node).getFirst();
        fixture.complete(topic, node, claimed, StoreFixture.retrying(Duration.ofHours(1)));

        fixture.push(topic, Insertion.SUPERSEDE, task("a"));

        assertThat(fixture.state(claimed.id())).isEqualTo("redundant");
    }

    @Test
    void replace_also_covers_parked_dead_and_finished_tasks() throws SQLException {
        StoreFixture fixture = setUp();
        long succeeded = fixture.push(topic, "a");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.succeeded());
        long dead = fixture.push(topic, "a");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.deadLetter());
        long parked = fixture.push(topic, Insertion.APPEND, suspended("a")).getFirst().id();
        long running = fixture.push(topic, "a");
        fixture.claim(topic, node);

        fixture.push(topic, Insertion.REPLACE, task("a"));

        assertThat(fixture.state(succeeded)).isEqualTo("redundant");
        assertThat(fixture.state(dead)).isEqualTo("redundant");
        assertThat(fixture.state(parked)).isEqualTo("redundant");
        assertThat(fixture.state(running)).isEqualTo("active");
        assertThat(fixture.view(succeeded).orElseThrow().previousOutcome()).isEqualTo("succeeded");
    }

    @Test
    void delete_removes_earlier_tasks_and_their_history() throws SQLException {
        StoreFixture fixture = setUp();
        long finished = fixture.push(topic, "a");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.succeeded());
        long running = fixture.push(topic, "x");
        fixture.claim(topic, node);
        long waiting = fixture.push(topic, "a");
        long keepOther = fixture.push(topic, "b");

        List<Pushed> pushed = fixture.push(topic, Insertion.DELETE, task("a"), task("x"), task("a"));

        assertThat(pushed).extracting(Pushed::status).containsExactly(Pushed.Status.REDUNDANT, Pushed.Status.QUEUED, Pushed.Status.QUEUED);
        assertThat(pushed.getFirst().id()).isNull();
        assertThat(fixture.state(finished)).isNull();
        assertThat(fixture.state(waiting)).isNull();
        assertThat(fixture.state(running)).isEqualTo("active");
        assertThat(fixture.state(keepOther)).isEqualTo("ready");
        assertThat(fixture.state(pushed.get(2).id())).isEqualTo("ready");
    }

    @Test
    void a_dedup_key_allows_one_unfinished_task() throws SQLException {
        StoreFixture fixture = setUp();
        List<Pushed> first = fixture.push(topic, Insertion.APPEND, deduplicated("a", "key-1"));
        List<Pushed> second = fixture.push(topic, Insertion.APPEND, deduplicated("a", "key-1"), deduplicated("b", "key-2"));

        assertThat(first).extracting(Pushed::status).containsExactly(Pushed.Status.QUEUED);
        assertThat(second).extracting(Pushed::status).containsExactly(Pushed.Status.DUPLICATE, Pushed.Status.QUEUED);
        assertThat(fixture.counts(topic).total()).isEqualTo(2);

        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.succeeded());
        assertThat(fixture.push(topic, Insertion.APPEND, deduplicated("a", "key-1"))).extracting(Pushed::status).containsExactly(Pushed.Status.QUEUED);
    }

    @Test
    void a_dedup_key_holds_against_a_concurrent_producer() throws Exception {
        StoreFixture fixture = setUp();
        try (StoreTransaction first = fixture.store().begin()) {
            assertThat(fixture.store().push(first, topic, Insertion.APPEND, List.of(deduplicated("a", "key")))).extracting(Pushed::status).containsExactly(Pushed.Status.QUEUED);

            CompletableFuture<List<Pushed>> concurrent = CompletableFuture.supplyAsync(() -> {
                try (StoreTransaction second = fixture.store().begin()) {
                    List<Pushed> result = fixture.store().push(second, topic, Insertion.APPEND, List.of(deduplicated("a", "key")));
                    second.commit();
                    return result;
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            });
            Thread.sleep(200);
            assertThat(concurrent).as("the second producer waits for the first").isNotDone();
            first.commit();

            assertThat(concurrent.get(5, TimeUnit.SECONDS)).extracting(Pushed::status).containsExactly(Pushed.Status.DUPLICATE);
        }
        assertThat(fixture.counts(topic).total()).isEqualTo(1);
    }

    @Test
    void a_rolled_back_push_leaves_nothing() throws SQLException {
        StoreFixture fixture = setUp();
        long id;
        try (StoreTransaction transaction = fixture.store().begin()) {
            id = fixture.store().push(transaction, topic, Insertion.APPEND, List.of(task("a"))).getFirst().id();
            transaction.rollback();
        }
        assertThat(fixture.state(id)).isNull();
        assertThat(fixture.counts(topic).total()).isZero();
    }

    @Test
    void lineage_and_reference_are_stored() throws SQLException {
        StoreFixture fixture = setUp();
        var child = new TaskStore.NewTask("a", null, "v1", "ref-1", null, false, 0, null, 10L, 7L, null);
        long id = fixture.push(topic, Insertion.APPEND, child).getFirst().id();

        TaskView view = fixture.view(id).orElseThrow();
        assertThat(view.reference()).isEqualTo("ref-1");
        assertThat(view.parentId()).isEqualTo(10L);
        assertThat(view.rootId()).isEqualTo(7L);
        assertThat(view.payloadType()).isEqualTo("v1");
    }

    @Test
    void topics_are_created_once() throws SQLException {
        StoreFixture fixture = setUp();
        assertThat(fixture.topic("topic")).isEqualTo(topic);
        java.util.OptionalInt found = fixture.inTransaction(transaction -> fixture.store().findTopic(transaction, "other"));
        java.util.OptionalInt missing = fixture.inTransaction(transaction -> fixture.store().findTopic(transaction, "missing"));
        assertThat(found).hasValue(other);
        assertThat(missing).isEmpty();
    }
}
