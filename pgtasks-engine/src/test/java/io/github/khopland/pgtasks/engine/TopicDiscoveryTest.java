package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.postgres.PostgresNotifier;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Topics paused and resumed at runtime, on the in-memory store and on Postgres. */
class TopicDiscoveryTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final Duration SOON = Duration.ofSeconds(1);
    private static final TopicRef<String> ASSESSMENT = TopicRef.of("assessment-2027", String.class);

    private final MemoryEngines engines = new MemoryEngines();
    private final Set<String> processed = ConcurrentHashMap.newKeySet();
    private EngineFixture fixture;

    @AfterEach
    void tearDown() {
        engines.close();
        if (fixture != null) {
            fixture.close();
        }
    }

    /** Records "topic/payload" for every task it runs. */
    private Topic<String> assessments() {
        return Topic.of(ASSESSMENT, batch -> batch.decide(task -> {
            processed.add(task.task().topic().name() + "/" + task.payload());
            return Decision.success();
        }));
    }

    private TaskEngine memoryNode(String name, List<Topic<?>> topics) throws Exception {
        TaskEngine engine = new TaskEngine(engines.store, MemoryEngines.settings(name).withNotifier(engines.store.notifier()), topics);
        return engines.add(engine).start();
    }

    private static void push(TaskEngine engine, String topic, String payload) throws Exception {
        engine.tasks().independently().push(TopicRef.of(topic, String.class), payload, payload);
    }

    /**
     * A pause's notification reconciles on another thread. It used to read the topics, then act
     * on what it read after the resume had started a new worker, and retire that one.
     */
    @Test
    void a_quick_pause_and_resume_leaves_the_topic_running() throws Exception {
        Topic<String> topic = Topic.of(TopicRef.of("topic", String.class), batch -> batch.decide(task -> Decision.success()));
        TaskEngine node = memoryNode("node-a", List.of(topic));
        for (int round = 0; round < 30; round++) {
            node.admin().pause("topic");
            node.admin().resume("topic");
            Thread.sleep(20);
            assertThat(node.processing()).as("after round %d", round).containsExactly("topic");
        }
    }

    @Test
    void a_paused_topic_takes_pushes_but_runs_nothing_until_resumed_on_every_node() throws Exception {
        Topic<String> topic = Topic.of(TopicRef.of("topic", String.class), batch -> batch.decide(task -> {
            processed.add(task.payload());
            return Decision.success();
        }));
        TaskEngine nodeA = memoryNode("node-a", List.of(topic));
        TaskEngine nodeB = memoryNode("node-b", List.of(topic));

        nodeA.admin().pause("topic");

        EngineFixture.await("node B to stop too", SOON, () -> nodeB.processing().isEmpty());
        assertThat(nodeA.processing()).isEmpty();
        push(nodeB, "topic", "while-paused");
        Thread.sleep(200);
        assertThat(processed).isEmpty();

        nodeB.admin().resume("topic");

        EngineFixture.await("the waiting task to run", SOON, () -> processed.contains("while-paused"));
        assertThat(nodeA.processing()).containsExactly("topic");
    }

    @Test
    void a_topic_paused_before_a_node_starts_is_not_run() throws Exception {
        engines.createTopics("topic");
        try (var transaction = engines.store.begin()) {
            engines.store.setPaused(transaction, "topic", true);
            transaction.commit();
        }
        TaskEngine engine = memoryNode("node-a", List.of(Topic.of(TopicRef.of("topic", String.class), batch -> batch.decide(task -> Decision.success()))));

        assertThat(engine.processing()).isEmpty();
        assertThatThrownBy(() -> engine.admin().pause("missing")).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * On Postgres, with heartbeats an hour apart: only the notification can carry a pause or a
     * resume to the other nodes, and it does within a second.
     */
    @Test
    void pauses_reach_every_node_within_a_second(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        List<TaskEngine> nodes = List.of(
            fixture.start("node-a", List.of(), settings -> settings.withHeartbeat(Duration.ofHours(1), Duration.ofHours(10))),
            fixture.start("node-b", List.of(assessments()), settings -> settings.withHeartbeat(Duration.ofHours(1), Duration.ofHours(10))),
            fixture.start("node-c", List.of(assessments()), settings -> settings.withHeartbeat(Duration.ofHours(1), Duration.ofHours(10))));
        TaskEngine runner = nodes.get(0);
        // Only a notification carries the news here, so every node must be listening first.
        EngineFixture.await("every node listening", Duration.ofSeconds(5), () -> nodes.stream().allMatch(TopicDiscoveryTest::listening));
        push(runner, ASSESSMENT.name(), "a");
        EngineFixture.await("its task to run", SOON, () -> processed.contains("assessment-2027/a"));

        runner.admin().pause(ASSESSMENT.name());
        EngineFixture.await("both to pause it", SOON, () -> nodes.get(1).processing().isEmpty() && nodes.get(2).processing().isEmpty());
        push(runner, ASSESSMENT.name(), "b");
        Thread.sleep(300);
        assertThat(processed).doesNotContain("assessment-2027/b");

        runner.admin().resume(ASSESSMENT.name());
        EngineFixture.await("the waiting task to run after the resume", SOON, () -> processed.contains("assessment-2027/b"));
    }

    private static boolean listening(TaskEngine engine) {
        try {
            return engine.health().notifications() == EngineHealth.Notifications.CONNECTED;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Without notifications, the heartbeat's comparison picks a pause up: a lost notification costs one heartbeat. */
    @Test
    void without_notifications_the_next_heartbeat_picks_a_pause_up(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        TaskEngine engine = fixture.start("node-a", List.of(assessments()), settings -> settings.withNotifier(null));
        EngineFixture.await("it to run the topic", SOON, () -> engine.processing().contains(ASSESSMENT.name()));

        database.execute("UPDATE pgtasks.topic SET paused = true WHERE name = 'assessment-2027'");

        EngineFixture.await("a heartbeat (200 ms) to pick it up", SOON, () -> engine.processing().isEmpty());
    }
}
