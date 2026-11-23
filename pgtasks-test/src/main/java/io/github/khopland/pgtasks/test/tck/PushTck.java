package io.github.khopland.pgtasks.test.tck;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.admin.QueueCounts;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static io.github.khopland.pgtasks.test.tck.StoreFixture.deduplicated;
import static io.github.khopland.pgtasks.test.tck.StoreFixture.delayed;
import static io.github.khopland.pgtasks.test.tck.StoreFixture.suspended;
import static io.github.khopland.pgtasks.test.tck.StoreFixture.task;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            TaskView view = fixture.view(StoreFixture.id(each)).orElseThrow();
            assertThat(view.topic()).isEqualTo("topic");
            assertThat(view.identifier()).isEqualTo(each.identifier());
            assertThat(view.state()).isEqualTo("ready");
            assertThat(view.payload()).isEqualTo("payload-" + each.identifier());
            assertThat(view.finished()).isFalse();
            assertThat(view.createdAt()).isNotNull();
        }
    }

    @Test
    void a_savepoint_undoes_a_push_and_keeps_the_rest() throws SQLException {
        StoreFixture fixture = setUp();
        List<Long> kept = fixture.inTransaction(transaction -> {
            long before = StoreFixture.id(fixture.store().push(transaction, topic, Insertion.APPEND, List.of(task("before"))).getFirst());
            StoreTransaction.Savepoint undone = transaction.savepoint();
            fixture.store().push(transaction, topic, Insertion.APPEND, List.of(task("undone")));
            undone.rollback();
            StoreTransaction.Savepoint released = transaction.savepoint();
            long after = StoreFixture.id(fixture.store().push(transaction, topic, Insertion.APPEND, List.of(task("after"))).getFirst());
            released.release();
            return List.of(before, after);
        });

        assertThat(fixture.inTransaction(transaction -> fixture.store().count(transaction, topic)).ready()).isEqualTo(2);
        assertThat(kept).allSatisfy(id -> assertThat(fixture.state(id)).isEqualTo("ready"));
    }

    @Test
    void a_push_with_a_run_time_waits_until_then_by_the_stores_clock() throws Exception {
        StoreFixture fixture = setUp();
        Instant now = fixture.store().clock().instant().truncatedTo(ChronoUnit.MILLIS);
        long later = StoreFixture.id(fixture.push(topic, Insertion.APPEND, StoreFixture.at("later", now.plus(Duration.ofHours(1)))).getFirst());
        long past = StoreFixture.id(fixture.push(topic, Insertion.APPEND, StoreFixture.at("past", now.minus(Duration.ofHours(1)))).getFirst());

        TaskView waiting = fixture.view(later).orElseThrow();
        assertThat(waiting.state()).isEqualTo("scheduled");
        assertThat(waiting.runAt()).as("the time given, not one counted from the push").isEqualTo(now.plus(Duration.ofHours(1)));
        assertThat(fixture.state(past)).as("a time that has come is ready at once").isEqualTo("ready");
        assertThat(fixture.view(past).orElseThrow().runAt()).isNull();
        assertThat(fixture.promoteDue()).isEmpty();

        fixture.advance(Duration.ofMinutes(61));
        assertThat(fixture.promoteDue()).containsExactly(topic);
        assertThat(fixture.state(later)).isEqualTo("ready");
    }

    @Test
    void delayed_and_suspended_pushes_wait() throws SQLException {
        StoreFixture fixture = setUp();
        Instant before = fixture.store().clock().instant();
        List<Pushed> pushed = fixture.push(topic, Insertion.APPEND, delayed("a", Duration.ofMinutes(5)), suspended("b"), delayed("c", Duration.ZERO));

        assertThat(pushed).extracting(Pushed::id).extracting(fixture::state).containsExactly("scheduled", "suspended", "ready");
        assertThat(fixture.view(StoreFixture.id(pushed.getFirst())).orElseThrow().runAt())
            .isBetween(before.plus(Duration.ofMinutes(4)), before.plus(Duration.ofMinutes(6)));
        QueueCounts counts = fixture.counts(topic);
        assertThat(counts).isEqualTo(new QueueCounts(1, 0, 1, 0, 1, 0, 0, counts.nextRunAt(), counts.oldestReadyAt(), 0, null));
        assertThat(counts.nextRunAt()).isEqualTo(fixture.view(StoreFixture.id(pushed.getFirst())).orElseThrow().runAt());
        assertThat(counts.oldestReadyAt()).isEqualTo(fixture.view(StoreFixture.id(pushed.get(2))).orElseThrow().createdAt());
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
        long delayedTask = StoreFixture.id(fixture.push(topic, Insertion.APPEND, delayed("a", Duration.ofHours(1))).getFirst());
        long parked = StoreFixture.id(fixture.push(topic, Insertion.APPEND, suspended("a")).getFirst());
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
        assertThat(fixture.state(StoreFixture.id(pushed.get(0)))).isEqualTo("redundant");
        assertThat(fixture.state(StoreFixture.id(pushed.get(1)))).isEqualTo("ready");
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
        long parked = StoreFixture.id(fixture.push(topic, Insertion.APPEND, suspended("a")).getFirst());
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

    /**
     * Order per identifier is task-id order, and ids are drawn before commit. The push that drew
     * the later id must still find the earlier task when the earlier push commits last.
     */
    @ParameterizedTest
    @EnumSource(value = Insertion.class, names = "APPEND", mode = EnumSource.Mode.EXCLUDE)
    void a_later_push_waits_for_an_earlier_push_of_the_identifier(Insertion insertion) throws Exception {
        StoreFixture fixture = setUp();
        long earlier;
        CompletableFuture<Long> later;
        try (StoreTransaction first = fixture.store().begin()) {
            earlier = StoreFixture.id(fixture.store().push(first, topic, insertion, List.of(task("x"))).getFirst());

            later = CompletableFuture.supplyAsync(() -> {
                try (StoreTransaction second = fixture.store().begin()) {
                    long id = StoreFixture.id(fixture.store().push(second, topic, insertion, List.of(task("x"))).getFirst());
                    second.commit();
                    return id;
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            });
            Thread.sleep(300);      // let the later push get as far as it can
            assertThat(later).as("the later push waits for the earlier one").isNotDone();
            first.commit();
        }

        long id = later.get(10, TimeUnit.SECONDS);
        assertThat(id).as("the later push's task").isGreaterThan(earlier);
        assertThat(fixture.state(id)).isEqualTo("ready");
        assertThat(fixture.state(earlier)).as("the earlier task").isNotEqualTo("ready");
        assertThat(fixture.counts(topic).ready()).isEqualTo(1);
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
            id = StoreFixture.id(fixture.store().push(transaction, topic, Insertion.APPEND, List.of(task("a"))).getFirst());
            transaction.rollback();
        }
        assertThat(fixture.state(id)).isNull();
        assertThat(fixture.counts(topic).total()).isZero();
    }

    @Test
    void lineage_and_reference_are_stored() throws SQLException {
        StoreFixture fixture = setUp();
        var child = new TaskStore.NewTask("a", null, "v1", "ref-1", null, false, 0, null, 10L, 7L, Map.of("korrelasjonsid", "k-1"));
        long id = StoreFixture.id(fixture.push(topic, Insertion.APPEND, child).getFirst());

        TaskView view = fixture.view(id).orElseThrow();
        assertThat(view.reference()).isEqualTo("ref-1");
        assertThat(view.parentId()).isEqualTo(10L);
        assertThat(view.rootId()).isEqualTo(7L);
        assertThat(view.payloadType()).isEqualTo("v1");
        assertThat(view.headers()).containsExactly(Map.entry("korrelasjonsid", "k-1"));
    }

    @Test
    void topics_are_created_once() throws SQLException {
        StoreFixture fixture = setUp();
        assertThat(fixture.topic("topic")).isEqualTo(topic);
        OptionalInt found = fixture.inTransaction(transaction -> fixture.store().findTopic(transaction, "other"));
        OptionalInt missing = fixture.inTransaction(transaction -> fixture.store().findTopic(transaction, "missing"));
        assertThat(found).hasValue(other);
        assertThat(missing).isEmpty();
    }
}
