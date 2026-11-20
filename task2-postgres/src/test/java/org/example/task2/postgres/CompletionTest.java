package org.example.task2.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.example.task2.postgres.StoreFixture.task;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import org.example.task2.Insertion;
import org.example.task2.spi.StaleClaimException;
import org.example.task2.spi.TaskStore;
import org.example.task2.spi.TaskStore.ClaimedTask;
import org.example.task2.spi.TaskStore.Completion;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class CompletionTest {

    @RegisterExtension
    static PostgresExtension postgres = StoreFixture.extension();

    private StoreFixture fixture;
    private int topic, node;

    @BeforeEach
    void setUp(PostgresDatabase database) throws SQLException {
        fixture = new StoreFixture(database);
        topic = fixture.topic("topic");
        node = fixture.node("node-a");
    }

    @Test
    void success_removes_the_queue_row_and_records_the_outcome() throws SQLException {
        long id = fixture.push(topic, "a");
        ClaimedTask claimed = fixture.claim(topic, node).getFirst();

        fixture.complete(topic, node, claimed, StoreFixture.succeeded());

        assertThat(fixture.database.count("SELECT count(*) FROM tasks.queue")).isZero();
        assertThat(fixture.database.rows("SELECT outcome::text, attempts, completed_at IS NOT NULL FROM tasks.task WHERE id = ?", id).getFirst())
            .containsExactly("succeeded", 1, true);
    }

    @Test
    void filtered_keeps_its_reason() throws SQLException {
        long id = fixture.push(topic, "a");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), new TaskStore.Resolution.Finished(TaskStore.Outcome.FILTERED, "irrelevant"));

        assertThat(fixture.database.rows("SELECT outcome::text, output FROM tasks.task WHERE id = ?", id).getFirst())
            .containsExactly("filtered", "irrelevant");
    }

    @Test
    void a_completion_with_a_stale_attempt_matches_nothing_and_must_roll_back() throws SQLException {
        long id = fixture.push(topic, "a");
        int crashed = fixture.node("crashed");
        ClaimedTask stale = fixture.claim(topic, crashed).getFirst();
        fixture.expireNode(crashed);
        fixture.inTransaction(connection -> fixture.store.requeueExpired(connection, node, Duration.ofMinutes(10)));
        ClaimedTask current = fixture.claim(topic, node).getFirst();
        assertThat(current.attempt()).isEqualTo(2);

        assertThatThrownBy(() -> fixture.inTransaction(connection -> {
            fixture.store.push(connection, topic, Insertion.APPEND, List.of(task("child-of-stale")));
            fixture.store.complete(connection, topic, crashed, List.of(new Completion(stale.id(), stale.attempt(), stale.claimedAt(), StoreFixture.succeeded())));
            return null;
        })).isInstanceOf(StaleClaimException.class).satisfies(e -> assertThat(((StaleClaimException) e).lost()).containsExactly(id));

        assertThat(fixture.state(id)).isEqualTo("active");
        assertThat(fixture.database.count("SELECT count(*) FROM tasks.task WHERE identifier = 'child-of-stale'")).isZero();

        fixture.complete(topic, node, current, StoreFixture.succeeded());
        assertThat(fixture.state(id)).isEqualTo("succeeded");
    }

    @Test
    void a_completion_is_all_or_nothing_across_the_batch() throws SQLException {
        fixture.push(topic, Insertion.APPEND, task("a"), task("b"));
        List<ClaimedTask> claimed = fixture.claim(topic, node);
        fixture.database.execute("DELETE FROM tasks.queue WHERE task_id = " + claimed.get(1).id());

        assertThatThrownBy(() -> fixture.inTransaction(connection -> {
            fixture.store.complete(connection, topic, node, claimed.stream()
                .map(c -> new Completion(c.id(), c.attempt(), c.claimedAt(), StoreFixture.succeeded()))
                .toList());
            return null;
        })).isInstanceOf(StaleClaimException.class);
        assertThat(fixture.state(claimed.getFirst().id())).isEqualTo("active");

        int released = fixture.inTransaction(connection -> fixture.store.release(connection, topic, node, claimed.stream()
            .map(c -> new Completion(c.id(), c.attempt(), c.claimedAt(), StoreFixture.retrying(Duration.ofSeconds(1))))
            .toList()));
        assertThat(released).isEqualTo(1);
    }

    @Test
    void another_nodes_completion_is_fenced_out() throws SQLException {
        fixture.push(topic, "a");
        ClaimedTask claimed = fixture.claim(topic, node).getFirst();
        int other = fixture.node("node-b");

        assertThatThrownBy(() -> fixture.complete(topic, other, claimed, StoreFixture.succeeded())).isInstanceOf(StaleClaimException.class);
    }

    @Test
    void a_failure_backs_off_counts_and_is_logged() throws SQLException {
        long id = fixture.push(topic, "a");
        ClaimedTask claimed = fixture.claim(topic, node).getFirst();

        fixture.complete(topic, node, claimed, StoreFixture.retrying(Duration.ofMinutes(1)));

        assertThat(fixture.state(id)).isEqualTo("retrying");
        assertThat(fixture.attempt(id)).isEqualTo(1);
        assertThat(fixture.database.count("SELECT count(*) FROM tasks.queue WHERE run_at BETWEEN now() + interval '50 seconds' AND now() + interval '70 seconds'")).isEqualTo(1);
        assertThat(fixture.database.rows("SELECT attempt, result, error->>'message', node_id FROM tasks.attempt WHERE task_id = ?", id))
            .containsExactly(List.of(1, "failed", "boom", node));
        assertThat(fixture.claim(topic, node)).as("not ready before its time").isEmpty();

        fixture.makeDue();
        List<Integer> promoted = fixture.inTransaction(connection -> fixture.store.promoteDue(connection, 100));
        assertThat(promoted).containsExactly(topic);
        ClaimedTask retry = fixture.claim(topic, node).getFirst();
        assertThat(retry.attempt()).isEqualTo(2);
    }

    @Test
    void a_dead_letter_stays_in_the_queue_with_its_error() throws SQLException {
        long id = fixture.push(topic, "a");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.deadLetter());

        assertThat(fixture.state(id)).isEqualTo("failed");
        assertThat(fixture.database.rows("SELECT outcome, error->>'message', attempts FROM tasks.task WHERE id = ?", id).getFirst())
            .containsExactly(null, "gave up", 1);
        assertThat(fixture.claim(topic, node)).isEmpty();
    }

    @Test
    void suspension_delay_and_throttling_give_the_attempt_back() throws SQLException {
        long suspended = fixture.push(topic, "s");
        long delayed = fixture.push(topic, "d");
        long throttled = fixture.push(topic, "t");
        List<ClaimedTask> claimed = fixture.claim(topic, node);
        assertThat(claimed).extracting(ClaimedTask::attempt).containsOnly(1);

        fixture.inTransaction(connection -> {
            fixture.store.complete(connection, topic, node, List.of(
                new Completion(claimed.get(0).id(), 1, claimed.get(0).claimedAt(), StoreFixture.suspension()),
                new Completion(claimed.get(1).id(), 1, claimed.get(1).claimedAt(), StoreFixture.later(Duration.ofSeconds(30))),
                new Completion(claimed.get(2).id(), 1, claimed.get(2).claimedAt(), StoreFixture.throttled(Duration.ofSeconds(30)))
            ));
            return null;
        });

        assertThat(List.of(suspended, delayed, throttled)).extracting(fixture::state).containsExactly("suspended", "scheduled", "retrying");
        assertThat(List.of(suspended, delayed, throttled)).extracting(fixture::attempt).containsOnly(0);
        assertThat(fixture.database.count("SELECT count(*) FROM tasks.attempt")).isZero();
    }

    @Test
    void expiry_requeues_other_nodes_tasks_keeping_the_attempt() throws SQLException {
        long mine = fixture.push(topic, "mine");
        fixture.claim(topic, node);
        long theirs = fixture.push(topic, "theirs");
        int crashed = fixture.node("crashed");
        fixture.claim(topic, crashed);
        fixture.expireNode(crashed);
        fixture.expireNode(node);

        List<Integer> topics = fixture.inTransaction(connection -> fixture.store.requeueExpired(connection, node, Duration.ofMinutes(10)));

        assertThat(topics).containsExactly(topic);
        assertThat(fixture.state(mine)).as("a node never expires itself").isEqualTo("active");
        assertThat(fixture.state(theirs)).isEqualTo("ready");
        assertThat(fixture.attempt(theirs)).isEqualTo(1);
        assertThat(fixture.database.rows("SELECT result, node_id FROM tasks.attempt WHERE task_id = ?", theirs)).containsExactly(List.of("expired", crashed));
    }

    @Test
    void a_fresh_heartbeat_prevents_expiry() throws SQLException {
        fixture.push(topic, "a");
        int other = fixture.node("node-b");
        fixture.claim(topic, other);
        fixture.expireNode(other);
        fixture.inTransaction(connection -> {
            fixture.store.heartbeat(connection, other);
            return null;
        });

        List<Integer> topics = fixture.inTransaction(connection -> fixture.store.requeueExpired(connection, node, Duration.ofMinutes(10)));
        assertThat(topics).isEmpty();
    }
}
