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

/** Topics created, paused and resumed at runtime, on the in-memory store and on Postgres. */
class TopicDiscoveryTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final Duration SOON = Duration.ofSeconds(1);

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
    private TopicTemplate<String> assessments() {
        return TopicTemplate.of("assessment-*", batch -> batch.decide(task -> {
            processed.add(task.task().topic().name() + "/" + task.payload());
            return Decision.success();
        }));
    }

    private TaskEngine memoryNode(String name, List<Topic<?>> topics, List<TopicTemplate<?>> templates) throws Exception {
        TaskEngine engine = new TaskEngine(engines.store, MemoryEngines.settings(name).withNotifier(engines.store.notifier()), topics, templates);
        return engines.add(engine).start();
    }

    private static void push(TaskEngine engine, String topic, String payload) throws Exception {
        engine.tasks().independently().push(TopicRef.of(topic, String.class), payload, payload);
    }

    @Test
    void a_topic_created_at_runtime_runs_on_every_node_with_a_matching_template() throws Exception {
        TaskEngine nodeA = memoryNode("node-a", List.of(), List.of(assessments()));
        TaskEngine nodeB = memoryNode("node-b", List.of(), List.of(assessments()));
        TaskEngine other = memoryNode("node-c", List.of(), List.of());

        other.admin().createTopic("assessment-2027");
        other.admin().createTopic("payroll-2027");

        EngineFixture.await("both template nodes to run it", SOON,
            () -> nodeA.processing().contains("assessment-2027") && nodeB.processing().contains("assessment-2027"));
        assertThat(other.processing()).as("no template, nothing to run").isEmpty();
        assertThat(nodeA.processing()).as("matches no template").doesNotContain("payroll-2027");
        push(other, "assessment-2027", "a");
        EngineFixture.await("the task to run", SOON, () -> processed.contains("assessment-2027/a"));
    }

    @Test
    void a_topic_registered_by_name_wins_over_a_template() throws Exception {
        Set<String> named = ConcurrentHashMap.newKeySet();
        TaskEngine engine = memoryNode("node-a",
            List.of(Topic.of(TopicRef.of("assessment-special", String.class), batch -> batch.decide(task -> {
                named.add(task.payload());
                return Decision.success();
            }))),
            List.of(assessments()));

        push(engine, "assessment-special", "x");

        EngineFixture.await("the named topic's handler", SOON, () -> named.contains("x"));
        assertThat(processed).isEmpty();
    }

    /**
     * A pause's notification reconciles on another thread. It used to read the topics, then act
     * on what it read after the resume had started a new worker, and retire that one.
     */
    @Test
    void a_quick_pause_and_resume_leaves_the_topic_running() throws Exception {
        Topic<String> topic = Topic.of(TopicRef.of("topic", String.class), batch -> batch.decide(task -> Decision.success()));
        TaskEngine node = memoryNode("node-a", List.of(topic), List.of());
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
        TaskEngine nodeA = memoryNode("node-a", List.of(topic), List.of());
        TaskEngine nodeB = memoryNode("node-b", List.of(topic), List.of());

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
        TaskEngine engine = memoryNode("node-a", List.of(Topic.of(TopicRef.of("topic", String.class), batch -> batch.decide(task -> Decision.success()))), List.of());

        assertThat(engine.processing()).isEmpty();
        assertThatThrownBy(() -> engine.admin().pause("missing")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> engine.admin().createTopic("Not A Topic")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void the_builder_registers_templates_and_overrides_address_the_pattern() {
        TaskEngine engine = TaskEngine.builder()
            .store(engines.store)
            .configure(tasks -> tasks.topicsMatching("assessment-*", String.class, batch -> batch.decide(task -> Decision.success()), topic -> topic.concurrency(3)))
            .overrides(Map.of("topics.assessment-*.batch-size", "7"))
            .build();

        TopicTemplate<?> template = engine.templates().getFirst();
        assertThat(template.pattern()).isEqualTo("assessment-*");
        assertThat(template.settings().concurrency()).isEqualTo(3);
        assertThat(template.settings().batchSize()).isEqualTo(7);
        assertThat(template.matches("assessment-2027")).isTrue();
        assertThat(template.matches("assessment")).isFalse();
        assertThatThrownBy(() -> TaskEngine.builder().store(engines.store)
            .configure(tasks -> tasks.topicsMatching("no-star", String.class, batch -> batch.decide(task -> Decision.success())))
            .build())
            .hasMessageContaining("Invalid topic pattern 'no-star'");
    }

    /**
     * On Postgres, with heartbeats an hour apart: only the notification can carry a new topic or a
     * pause to the other nodes, and it does within a second.
     */
    @Test
    void new_topics_and_pauses_reach_every_node_within_a_second(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        List<TaskEngine> nodes = List.of(
            fixture.start("node-a", List.of(), settings -> settings.withHeartbeat(Duration.ofHours(1), Duration.ofHours(10))),
            fixture.start("node-b", List.of(), settings -> settings.withHeartbeat(Duration.ofHours(1), Duration.ofHours(10))));
        TaskEngine runner = fixture.engines().getFirst();
        // EngineFixture takes topics only; nodes C and D run the template.
        TaskEngine withTemplate = new TaskEngine(new PostgresTaskStore(database.newPool("node-c", 4)),
            EngineFixture.settings("node-c").withHeartbeat(Duration.ofHours(1), Duration.ofHours(10))
                .withNotifier(new PostgresNotifier(database::connect)),
            List.of(), List.of(assessments())).start();
        TaskEngine withTemplateToo = new TaskEngine(new PostgresTaskStore(database.newPool("node-d", 4)),
            EngineFixture.settings("node-d").withHeartbeat(Duration.ofHours(1), Duration.ofHours(10))
                .withNotifier(new PostgresNotifier(database::connect)),
            List.of(), List.of(assessments())).start();
        try {
            // Only a notification carries the news here, so every node must be listening first.
            List<TaskEngine> all = List.of(nodes.get(0), nodes.get(1), withTemplate, withTemplateToo);
            EngineFixture.await("every node listening", Duration.ofSeconds(5), () -> all.stream().allMatch(TopicDiscoveryTest::listening));
            runner.admin().createTopic("assessment-2027");
            EngineFixture.await("both template nodes to run the new topic", SOON,
                () -> withTemplate.processing().contains("assessment-2027") && withTemplateToo.processing().contains("assessment-2027"));
            assertThat(nodes.get(1).processing()).isEmpty();
            push(runner, "assessment-2027", "a");
            EngineFixture.await("its task to run", SOON, () -> processed.contains("assessment-2027/a"));

            nodes.get(1).admin().pause("assessment-2027");
            EngineFixture.await("both to pause it", SOON,
                () -> withTemplate.processing().isEmpty() && withTemplateToo.processing().isEmpty());
            push(runner, "assessment-2027", "b");
            Thread.sleep(300);
            assertThat(processed).doesNotContain("assessment-2027/b");

            nodes.get(1).admin().resume("assessment-2027");
            EngineFixture.await("the waiting task to run after the resume", SOON, () -> processed.contains("assessment-2027/b"));
        } finally {
            withTemplate.stop(Duration.ofSeconds(5));
            withTemplateToo.stop(Duration.ofSeconds(5));
        }
    }

    private static boolean listening(TaskEngine engine) {
        try {
            return engine.health().notifications() == EngineHealth.Notifications.CONNECTED;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Without notifications, the heartbeat's comparison picks a new topic up: a lost notification costs one heartbeat. */
    @Test
    void without_notifications_the_next_heartbeat_picks_a_new_topic_up(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        TaskEngine engine = new TaskEngine(new PostgresTaskStore(database.newPool("node-a", 4)),
            EngineFixture.settings("node-a").withNotifier(null), List.of(), List.of(assessments())).start();
        try {
            database.execute("SELECT tasks.create_topic('assessment-2028')");

            EngineFixture.await("a heartbeat (200 ms) to pick it up", SOON, () -> engine.processing().contains("assessment-2028"));
        } finally {
            engine.stop(Duration.ofSeconds(5));
        }
    }
}
