package io.github.khopland.pgtasks.engine;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.github.khopland.pgtasks.BatchHandler;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Outcome;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.DeadLetterQuery;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.admin.TopicStatus;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.postgres.PostgresNotifier;
import io.github.khopland.pgtasks.spi.ManualClock;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The operator's side of the engine: topics, dead letters, purging and health. */
class AdminTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private TaskEngine engine;
    private EngineFixture fixture;

    @AfterEach
    void stop() {
        if (engine != null) {
            engine.close();
        }
        if (fixture != null) {
            fixture.close();
        }
    }

    /** Fails each payload starting with "flaky" once, so it is dead-lettered (no retries) and works when revived. */
    private TaskEngine start(InMemoryTaskStore store) throws Exception {
        Set<String> failedOnce = ConcurrentHashMap.newKeySet();
        engine = TaskEngine.builder()
            .store(store)
            .configure("work-module", tasks -> tasks.topic(WORK, batch -> batch.decide(task ->
                task.payload().startsWith("flaky") && failedOnce.add(task.payload()) ? Decision.failed("not yet") : Decision.success()),
                topic -> topic.retry(Retry.none())))
            .build()
            .start();
        return engine;
    }

    @Test
    @DisplayName("Dead letters can be listed, revived with fresh attempts, or discarded")
    void dead_letters_are_listed_revived_and_discarded() throws Exception {
        start(new InMemoryTaskStore());
        TaskTester tester = TaskTester.of(engine);
        long revived = engine.tasks().independently().push(WORK, "a", "flaky-a").id();
        long discarded = engine.tasks().independently().push(WORK, "b", "flaky-b").id();
        tester.awaitIdle();

        List<TaskView> dead = engine.admin().deadLetters("work", 0, 10);
        assertThat(dead).extracting(TaskView::id).containsExactly(revived, discarded);
        assertThat(dead.getFirst().error()).contains("not yet");

        assertThat(engine.admin().revive("work", List.of(revived))).isEqualTo(1);
        tester.awaitIdle();
        assertThat(engine.admin().find(revived).orElseThrow().state()).isEqualTo("succeeded");

        assertThat(engine.admin().discard("work", DeadLetterQuery.ALL)).isEqualTo(1);
        TaskView gone = engine.admin().find(discarded).orElseThrow();
        assertThat(gone.finished()).isTrue();
        assertThat(gone.state()).isEqualTo("failed");
        assertThat(engine.admin().deadLetters("work", 0, 10)).isEmpty();
        assertThatThrownBy(() -> engine.admin().revive("missing", DeadLetterQuery.ALL)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("missing");
    }

    @Test
    @DisplayName("Purging deletes one outcome's finished tasks up to a time")
    void purge_deletes_finished_tasks_of_one_outcome() throws Exception {
        start(new InMemoryTaskStore());
        long done = engine.tasks().independently().push(WORK, "a", "fine").id();
        engine.tasks().independently().push(WORK, "b", "flaky-b");
        TaskTester.of(engine).awaitIdle();

        assertThat(engine.admin().purge("work", Outcome.FAILED, Instant.now().plusSeconds(1))).as("the dead letter isn't finished").isZero();
        assertThat(engine.admin().purge("work", Outcome.SUCCEEDED, Instant.now().plusSeconds(1))).isEqualTo(1);
        assertThat(engine.admin().find(done)).isEmpty();
    }

    @Test
    @DisplayName("Topic status: every topic with its pause flag, module and whether this node runs it")
    void topic_status_lists_every_topic() throws Exception {
        start(new InMemoryTaskStore());
        engine.admin().createTopic("elsewhere");
        engine.admin().pause("elsewhere");

        assertThat(engine.admin().topics()).extracting(TopicStatus::name, TopicStatus::paused,
                TopicStatus::processing, TopicStatus::module)
            .containsExactly(
                Tuple.tuple("elsewhere", true, false, null),
                Tuple.tuple("work", false, true, "work-module"));
    }

    @Test
    @DisplayName("Health on Postgres: notifications are connected, and show when they can't be")
    void health_shows_whether_notifications_get_through(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        TaskEngine connected = fixture.start("node-a", List.of());
        EngineFixture.await("the notifier connected", Duration.ofSeconds(5),
            () -> health(connected).notifications() == EngineHealth.Notifications.CONNECTED);
        assertThat(health(connected).healthy()).isTrue();

        TaskEngine cutOff = fixture.start("node-b", List.of(), settings -> settings.withNotifier(new PostgresNotifier(() -> {
            throw new SQLException("no route to the database");
        })));
        EngineFixture.await("the notifier gave up for now", Duration.ofSeconds(5),
            () -> health(cutOff).notifications() == EngineHealth.Notifications.DISCONNECTED);
        assertThat(health(cutOff).healthy()).as("polling still works").isTrue();
    }

    private static EngineHealth health(TaskEngine engine) {
        try {
            return engine.health();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    @DisplayName("Health: a late heartbeat and claims past their deadline show")
    void health_shows_a_late_heartbeat_and_stuck_claims() throws Exception {
        ManualClock clock = new ManualClock(Instant.parse("2026-11-21T10:00:00Z"));
        CountDownLatch release = new CountDownLatch(1), running = new CountDownLatch(1);
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .configure("work", tasks -> tasks.topic(WORK, batch -> {
                running.countDown();
                release.await();
                return batch.decide(task -> Decision.success());
            }))
            .build();
        assertThat(engine.health().running()).isFalse();
        engine.start();

        EngineHealth healthy = engine.health();
        assertThat(healthy.healthy()).isTrue();
        assertThat(healthy.notifications()).isNotEqualTo(EngineHealth.Notifications.DISCONNECTED);
        assertThat(healthy.stuck()).isEmpty();

        long id = engine.tasks().independently().push(WORK, "a", "slow").id();
        assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();
        clock.advance(Duration.ofHours(1));

        EngineHealth late = engine.health();
        assertThat(late.heartbeatOnTime()).isFalse();
        assertThat(late.healthy()).isFalse();
        assertThat(late.stuck()).singleElement().satisfies(stuck -> {
            assertThat(stuck.topic()).isEqualTo("work");
            assertThat(stuck.taskId()).isEqualTo(id);
            assertThat(stuck.running()).isGreaterThanOrEqualTo(Duration.ofHours(1));
        });
        release.countDown();
    }

    /** Review 2026-11-21: a topic no longer used could never be removed. */
    @Test
    void a_topic_no_longer_used_is_deleted_with_its_history_on_every_node() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        TopicRef<String> retired = TopicRef.of("retired-2025", String.class);
        try (TaskEngine one = TaskEngine.builder().store(store).configure("work", tasks -> tasks
                .topicsMatching("retired-*", String.class, BatchHandler.each(task -> { })))
                .build().start();
             TaskEngine other = TaskEngine.builder().store(store).configure("work", tasks -> tasks
                .topicsMatching("retired-*", String.class, BatchHandler.each(task -> { })))
                .build().start()) {
            one.admin().createTopic("retired-2025");
            one.tasks().independently().push(retired, "a", "done");
            TaskTester.of(one, other).awaitIdle();
            assertThat(other.processing()).contains("retired-2025");

            assertThat(one.admin().deleteTopic("retired-2025")).isEqualTo(1);

            assertThat(one.admin().topic("retired-2025")).isEmpty();
            EngineFixture.await("the other node stops it", Duration.ofSeconds(5), () -> !other.processing().contains("retired-2025"));
            assertThatThrownBy(() -> other.tasks().independently().push(retired, "b", "too late")).hasMessageContaining("Unknown topic 'retired-2025'");
        }
    }

    @Test
    void a_topic_with_unfinished_tasks_is_not_deleted() throws Exception {
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore())
            .configure("work", tasks -> tasks.eachTask(WORK, task -> { })).build().start()) {
            engine.admin().pause("work");
            engine.tasks().independently().push(WORK, "a", "waiting");

            assertThatThrownBy(() -> engine.admin().deleteTopic("work")).hasMessageContaining("has 1 unfinished task(s)");
            assertThat(engine.admin().topic("work")).isPresent();
        }
    }
}
