package org.example.task2.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;

import org.example.task2.Insertion;
import org.example.task2.Ordering;
import org.example.task2.spi.TaskStore;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.example.task2.test.tck.StoreFixture;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * A topic that polled empty for a while must claim quickly once work arrives. With a cached
 * generic plan built while the queue was empty, claims were about 30 times slower (found by the
 * Phase 2 benchmark); claims are now planned for their actual parameters every time.
 */
class ClaimPlanTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresStoreFixture.extension();

    @ParameterizedTest
    @EnumSource(value = Ordering.class, names = {"FIFO", "SINGULAR_BY_IDENTIFIER"})
    void claims_stay_fast_after_a_topic_sat_idle(Ordering ordering, PostgresDatabase database) throws SQLException {
        PostgresStoreFixture fixture = new PostgresStoreFixture(database);
        int topic = fixture.topic("topic");
        int node = fixture.node("node-a");
        for (int poll = 0; poll < 500; poll++) {
            assertThat(fixture.claim(topic, node, ordering, 100)).isEmpty();
        }
        TaskStore.NewTask[] burst = Arrays.stream(StoreFixture.many("t", 10_000))
            .map(task -> new TaskStore.NewTask("id-" + (task.identifier().hashCode() & 0x3ff), null, null, null, null, false, 0, null, null, null, null))
            .toArray(TaskStore.NewTask[]::new);
        fixture.push(topic, Insertion.APPEND, burst);

        long started = System.nanoTime();
        int claimed = 0;
        while (claimed < 10_000) {
            var tasks = fixture.claim(topic, node, ordering, 100);
            fixture.inTransaction(transaction -> {
                fixture.store().complete(transaction, topic, node, tasks.stream().map(task -> StoreFixture.completion(task, StoreFixture.succeeded())).toList());
                return null;
            });
            claimed += tasks.size();
        }
        Duration elapsed = Duration.ofNanos(System.nanoTime() - started);

        assertThat(elapsed).as("draining 10,000 tasks after 500 idle polls").isLessThan(Duration.ofSeconds(8));
    }

    /**
     * A queue is often empty when autovacuum analyzes it: right after a burst drained. The
     * statistics then say the table is empty while it still has pages, so the planner expects
     * no rows at all until the next analyze. A per-identifier claim must still look up each
     * candidate's identifier instead of scanning every unfinished task for each candidate.
     */
    @ParameterizedTest
    @EnumSource(value = Ordering.class, names = {"SINGULAR_BY_IDENTIFIER", "SINGULAR_BY_IDENTIFIER_ON_FAILURE", "SINGULAR_BY_IDENTIFIER_UNTIL_SUCCESS"})
    void claims_stay_fast_after_the_queue_was_analyzed_empty(Ordering ordering, PostgresDatabase database) throws SQLException {
        PostgresStoreFixture fixture = new PostgresStoreFixture(database);
        int topic = fixture.topic("topic");
        int node = fixture.node("node-a");
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("warm-up", 20_000));
        drain(fixture, topic, node, Ordering.FIFO, 20_000);
        database.execute("ANALYZE tasks.queue");
        TaskStore.NewTask[] burst = new TaskStore.NewTask[20_000];
        for (int index = 0; index < burst.length; index++) {
            burst[index] = StoreFixture.task("id-" + index % 5_000);
        }
        fixture.push(topic, Insertion.APPEND, burst);

        long started = System.nanoTime();
        drain(fixture, topic, node, ordering, 20_000);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - started);

        assertThat(elapsed).as("draining 20,000 tasks for 5,000 identifiers on a queue analyzed while empty").isLessThan(Duration.ofSeconds(8));
    }

    private static void drain(PostgresStoreFixture fixture, int topic, int node, Ordering ordering, int count) throws SQLException {
        int claimed = 0;
        while (claimed < count) {
            var tasks = fixture.claim(topic, node, ordering, 100);
            fixture.inTransaction(transaction -> {
                fixture.store().complete(transaction, topic, node, tasks.stream().map(task -> StoreFixture.completion(task, StoreFixture.succeeded())).toList());
                return null;
            });
            claimed += tasks.size();
        }
    }
}
