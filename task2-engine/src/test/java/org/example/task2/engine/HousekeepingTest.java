package org.example.task2.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Period;
import java.util.List;
import java.util.Map;

import org.example.task2.Decision;
import org.example.task2.Outcome;
import org.example.task2.TopicRef;
import org.example.task2.spi.TaskStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The housekeeper on the in-memory store: one leader among several engines, failover, and the leader's jobs. */
class HousekeepingTest {

    private static final TopicRef<String> TOPIC = TopicRef.of("topic", String.class);

    private final MemoryEngines engines = new MemoryEngines();

    @AfterEach
    void tearDown() {
        engines.close();
    }

    private static List<Topic<?>> topic() {
        return List.of(Topic.of(TOPIC, batch -> batch.decide(task -> task.payload().equals("fail") ? Decision.failed("no") : task.payload().equals("filter")
            ? Decision.filtered("not needed") : Decision.success())));
    }

    private long leaders(TaskEngine... nodes) {
        return List.of(nodes).stream().filter(TaskEngine::leadsHousekeeping).count();
    }

    /** Review F1: promotion stopped after one batch of 1,000 unless 1,000 topics were due. */
    @Test
    void one_housekeeping_run_promotes_every_due_task_not_just_one_batch() throws Exception {
        TaskEngine engine = engines.start("node-a", topic());
        engine.pause("topic");
        engine.tasks().independently().push(TOPIC, org.example.task2.Insertion.APPEND, java.util.stream.IntStream.range(0, 2_500)
            .mapToObj(index -> org.example.task2.Creation.of("t" + index, "ok").withDelay(Duration.ofMinutes(1))).toList());
        TaskTester tester = TaskTester.of(engine);

        tester.advance(Duration.ofMinutes(2));

        assertThat(tester.counts(TOPIC).ready()).isEqualTo(2_500);
        assertThat(tester.counts(TOPIC).scheduled()).isZero();
    }

    @Test
    void one_engine_leads_and_another_takes_over_when_it_stops() throws Exception {
        TaskEngine nodeA = engines.start("node-a", topic());
        TaskEngine nodeB = engines.start("node-b", topic());
        TaskEngine nodeC = engines.start("node-c", topic());
        EngineFixture.await("one leader", Duration.ofSeconds(5), () -> leaders(nodeA, nodeB, nodeC) == 1);
        TaskEngine leader = List.of(nodeA, nodeB, nodeC).stream().filter(TaskEngine::leadsHousekeeping).findFirst().orElseThrow();

        leader.stop(Duration.ofSeconds(5));

        List<TaskEngine> rest = List.of(nodeA, nodeB, nodeC).stream().filter(engine -> engine != leader).toList();
        EngineFixture.await("another leader, without waiting for the term", Duration.ofSeconds(5),
            () -> leaders(rest.toArray(TaskEngine[]::new)) == 1);
        assertThat(leader.leadsHousekeeping()).isFalse();
    }

    @Test
    void a_leader_that_stops_renewing_loses_the_lease_once_its_term_runs_out() throws Exception {
        // Node A heartbeats once an hour, as if it hung right after starting; its lease lasts four heartbeats.
        TaskEngine hung = engines.start("node-a", topic(), settings -> settings.withHeartbeat(Duration.ofHours(1), Duration.ofHours(10)));
        EngineFixture.await("A to lead", Duration.ofSeconds(5), hung::leadsHousekeeping);
        TaskEngine nodeB = engines.start("node-b", topic());
        Thread.sleep(200);
        assertThat(nodeB.leadsHousekeeping()).as("A's term hasn't run out").isFalse();

        engines.clock.advance(Duration.ofHours(4).plusSeconds(1));

        EngineFixture.await("B to take over", Duration.ofSeconds(5), nodeB::leadsHousekeeping);
        assertThat(hung.leadsHousekeeping()).as("A knows its term ran out").isFalse();
    }

    @Test
    void only_the_leader_requeues_promotes_and_snapshots_and_a_new_leader_carries_on() throws Exception {
        TaskEngine nodeA = engines.start("node-a", topic());
        EngineFixture.await("A to lead", Duration.ofSeconds(5), nodeA::leadsHousekeeping);
        TaskEngine nodeB = engines.start("node-b", topic());
        TaskTester tester = engines.tester();

        long delayed = nodeA.tasks().independently().push(TOPIC, org.example.task2.Insertion.APPEND,
            List.of(org.example.task2.Creation.of("later", "ok").withDelay(Duration.ofMinutes(5)))).getFirst().id();
        nodeA.stop(Duration.ofSeconds(5));
        EngineFixture.await("B to lead", Duration.ofSeconds(5), nodeB::leadsHousekeeping);

        tester.advance(Duration.ofMinutes(6));
        tester.awaitIdle();

        assertThat(tester.find(delayed).orElseThrow().state()).as("promoted by the new leader and run").isEqualTo("succeeded");
        List<TaskStore.TopicSnapshot> snapshots = EngineCore.transact(engines.store, transaction -> engines.store.snapshots(transaction));
        assertThat(snapshots).extracting(TaskStore.TopicSnapshot::topic).contains(TOPIC.name());
    }

    @Test
    void retention_deletes_finished_tasks_per_outcome_counted_from_completion() throws Exception {
        TaskEngine engine = engines.start("node-a", List.of(topic().getFirst().with(TopicSettings.defaults()
            .withRetry(Retry.none())
            .withRetention(Map.of(Outcome.SUCCEEDED, Period.ofDays(30), Outcome.FILTERED, Period.ofDays(7))))));
        TaskTester tester = engines.tester();
        long succeeded = engine.tasks().independently().push(TOPIC, "a", "ok").id();
        long filtered = engine.tasks().independently().push(TOPIC, "b", "filter").id();
        long dead = engine.tasks().independently().push(TOPIC, "c", "fail").id();
        tester.awaitIdle();
        assertThat(tester.find(dead).orElseThrow().state()).isEqualTo("failed");

        tester.advance(Duration.ofDays(8));
        assertThat(tester.find(filtered)).as("filtered, past 7 days").isEmpty();
        assertThat(tester.find(succeeded)).as("succeeded, within 30 days").isPresent();

        tester.advance(Duration.ofDays(23));
        assertThat(tester.find(succeeded)).as("succeeded, past 30 days").isEmpty();
        assertThat(tester.find(dead)).as("a dead letter is unfinished and stays").isPresent();
    }

    @Test
    void the_leader_applies_retention_to_topics_it_does_not_run() throws Exception {
        TopicRef<String> other = TopicRef.of("other", String.class);
        TaskEngine leader = engines.start("node-a", topic());
        EngineFixture.await("A to lead", Duration.ofSeconds(5), leader::leadsHousekeeping);
        TaskEngine runner = engines.start("node-b", List.of(Topic.of(other, batch -> batch.decide(task -> Decision.success()))
            .with(TopicSettings.defaults().withRetention(Map.of(Outcome.SUCCEEDED, Period.ofDays(7))))));
        TaskTester tester = engines.tester();
        long id = runner.tasks().independently().push(other, "a", "ok").id();
        tester.awaitIdle();
        assertThat(tester.find(id).orElseThrow().state()).isEqualTo("succeeded");

        tester.advance(Duration.ofDays(8));

        assertThat(leader.leadsHousekeeping()).isTrue();
        assertThat(tester.find(id)).as("purged by node A, which doesn't run the topic").isEmpty();
    }

    @Test
    void retention_counts_from_completion_not_from_creation() throws Exception {
        TaskEngine engine = engines.start("node-a", List.of(topic().getFirst().with(TopicSettings.defaults()
            .withRetention(Map.of(Outcome.SUCCEEDED, Period.ofDays(30))))));
        TaskTester tester = engines.tester();
        long id = engine.tasks().independently().push(TOPIC, org.example.task2.Insertion.APPEND,
            List.of(org.example.task2.Creation.of("a", "ok").withDelay(Duration.ofDays(20)))).getFirst().id();

        tester.advance(Duration.ofDays(21));   // runs now, 21 days after it was created
        tester.awaitIdle();
        tester.advance(Duration.ofDays(20));   // 41 days after creation, 20 after completion

        assertThat(tester.find(id)).isPresent();
        tester.advance(Duration.ofDays(11));
        assertThat(tester.find(id)).isEmpty();
    }
}
