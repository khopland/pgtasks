package org.example.task2.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.example.task2.postgres.StoreFixture.deduplicated;
import static org.example.task2.postgres.StoreFixture.delayed;
import static org.example.task2.postgres.StoreFixture.suspended;
import static org.example.task2.postgres.StoreFixture.task;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.example.task2.Insertion;
import org.example.task2.Pushed;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class PushTest {

    @RegisterExtension
    static PostgresExtension postgres = StoreFixture.extension();

    private StoreFixture fixture;
    private int topic, other, node;

    @BeforeEach
    void setUp(PostgresDatabase database) throws SQLException {
        fixture = new StoreFixture(database);
        topic = fixture.topic("topic");
        other = fixture.topic("other");
        node = fixture.node("node-a");
    }

    @Test
    void append_stores_and_queues_in_order() throws SQLException {
        List<Pushed> pushed = fixture.push(topic, Insertion.APPEND, task("a"), task("b"), task("a"));

        assertThat(pushed).extracting(Pushed::status).containsOnly(Pushed.Status.QUEUED);
        assertThat(pushed).extracting(Pushed::id).isSorted().doesNotHaveDuplicates();
        assertThat(fixture.database.rows("SELECT identifier, state, payload FROM tasks.task_view ORDER BY id"))
            .containsExactly(List.of("a", "ready", "payload-a"), List.of("b", "ready", "payload-b"), List.of("a", "ready", "payload-a"));
    }

    @Test
    void delayed_and_suspended_pushes_wait() throws SQLException {
        List<Pushed> pushed = fixture.push(topic, Insertion.APPEND, delayed("a", Duration.ofMinutes(5)), suspended("b"), delayed("c", Duration.ZERO));

        assertThat(pushed).extracting(Pushed::id).extracting(fixture::state).containsExactly("scheduled", "suspended", "ready");
        assertThat(fixture.database.count("SELECT count(*) FROM tasks.queue WHERE run_at BETWEEN now() + interval '4 minutes' AND now() + interval '6 minutes'"))
            .isEqualTo(1);
    }

    @Test
    void push_to_an_unknown_topic_fails() {
        assertThatThrownBy(() -> fixture.push(9999, Insertion.APPEND, task("a"))).isInstanceOf(SQLException.class);
    }

    @Test
    void supersede_makes_waiting_tasks_redundant_but_not_running_or_parked_ones() throws SQLException {
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
    }

    @Test
    void supersede_covers_a_task_that_is_backing_off() throws SQLException {
        fixture.push(topic, "a");
        var claimed = fixture.claim(topic, node).getFirst();
        fixture.complete(topic, node, claimed, StoreFixture.retrying(Duration.ofHours(1)));

        fixture.push(topic, Insertion.SUPERSEDE, task("a"));

        assertThat(fixture.state(claimed.id())).isEqualTo("redundant");
    }

    @Test
    void replace_also_covers_parked_dead_and_finished_tasks() throws SQLException {
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
        assertThat(fixture.database.rows("SELECT previous_outcome::text FROM tasks.task WHERE id = ?", succeeded).getFirst().getFirst())
            .isEqualTo("succeeded");
    }

    @Test
    void delete_removes_earlier_tasks_and_their_history() throws SQLException {
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
        assertThat(fixture.database.count("SELECT count(*) FROM tasks.task WHERE identifier = 'a'")).isEqualTo(1);
    }

    @Test
    void a_dedup_key_allows_one_unfinished_task() throws SQLException {
        List<Pushed> first = fixture.push(topic, Insertion.APPEND, deduplicated("a", "key-1"));
        List<Pushed> second = fixture.push(topic, Insertion.APPEND, deduplicated("a", "key-1"), deduplicated("b", "key-2"));

        assertThat(first).extracting(Pushed::status).containsExactly(Pushed.Status.QUEUED);
        assertThat(second).extracting(Pushed::status).containsExactly(Pushed.Status.DUPLICATE, Pushed.Status.QUEUED);
        assertThat(fixture.database.count("SELECT count(*) FROM tasks.task")).isEqualTo(2);

        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.succeeded());
        assertThat(fixture.push(topic, Insertion.APPEND, deduplicated("a", "key-1"))).extracting(Pushed::status).containsExactly(Pushed.Status.QUEUED);
    }

    @Test
    void a_dedup_key_holds_against_a_concurrent_producer() throws Exception {
        try (Connection first = fixture.database.dataSource().getConnection(); Connection second = fixture.database.dataSource().getConnection()) {
            first.setAutoCommit(false);
            second.setAutoCommit(false);
            assertThat(fixture.store.push(first, topic, Insertion.APPEND, List.of(deduplicated("a", "key")))).extracting(Pushed::status).containsExactly(Pushed.Status.QUEUED);

            CompletableFuture<List<Pushed>> concurrent = CompletableFuture.supplyAsync(() -> {
                try {
                    List<Pushed> result = fixture.store.push(second, topic, Insertion.APPEND, List.of(deduplicated("a", "key")));
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
        assertThat(fixture.database.count("SELECT count(*) FROM tasks.queue")).isEqualTo(1);
    }

    @Test
    void lineage_and_reference_are_stored() throws SQLException {
        var child = new org.example.task2.spi.TaskStore.NewTask("a", null, "v1", "ref-1", null, false, 0, null, 10L, 7L);
        long id = fixture.push(topic, Insertion.APPEND, child).getFirst().id();

        assertThat(fixture.database.rows("SELECT reference, parent_id, root_id, payload_type FROM tasks.task WHERE id = ?", id).getFirst())
            .containsExactly("ref-1", 10L, 7L, "v1");
    }
}
