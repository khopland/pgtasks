package io.github.khopland.pgtasks.engine;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.Outcome;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.DeadLetterQuery;
import io.github.khopland.pgtasks.admin.QueueState;
import io.github.khopland.pgtasks.admin.TaskQuery;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.admin.TopicStatus;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.postgres.PostgresNotifier;
import io.github.khopland.pgtasks.spi.ManualClock;
import io.github.khopland.pgtasks.spi.StoreTransaction;
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
    @DisplayName("A task its handler suspended runs again once an operator releases it")
    void suspended_tasks_run_again_once_released() throws Exception {
        Set<String> ready = ConcurrentHashMap.newKeySet();
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .configure("work-module", tasks -> tasks.topic(WORK, batch -> batch.decide(
                task -> ready.contains(task.payload()) ? Decision.success() : Decision.suspended())))
            .build()
            .start();
        TaskTester tester = TaskTester.of(engine);
        long first = engine.tasks().independently().push(WORK, "a", "first").id();
        long second = engine.tasks().independently().push(WORK, "b", "second").id();
        tester.awaitIdle();
        assertThat(engine.admin().tasks("work", TaskQuery.ALL.inStates(QueueState.SUSPENDED), 10)).extracting(TaskView::id)
            .containsExactly(first, second);

        ready.add("first");
        assertThat(engine.admin().release("work", List.of(first, 999_999L))).isEqualTo(1);
        assertThat(engine.admin().release("work", TaskQuery.ALL.inStates(QueueState.READY))).as("asks for no suspended task").isZero();
        tester.awaitIdle();

        assertThat(engine.admin().find(first).orElseThrow().state()).isEqualTo("succeeded");
        assertThat(engine.admin().find(first).orElseThrow().attempt()).as("a suspension uses up no attempt").isEqualTo(1);
        assertThat(engine.admin().find(second).orElseThrow().state()).isEqualTo("suspended");
        assertThatThrownBy(() -> engine.admin().release("missing", TaskQuery.ALL)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("A cancelled task never runs, and the task it held back under an ordering runs next")
    void cancelled_tasks_never_run() throws Exception {
        List<String> ran = new CopyOnWriteArrayList<>();
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(ManualClock.startingNow()))
            .configure("work-module", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                ran.add(task.payload());
                return task.payload().equals("failing") ? Decision.failed("not yet") : Decision.success();
            }), topic -> topic.ordering(Ordering.SINGULAR_BY_IDENTIFIER)))
            .build()
            .start();
        TaskTester tester = TaskTester.of(engine);
        long failing = engine.tasks().independently().push(WORK, "order-1", "failing").id();
        tester.awaitIdle();
        long held = engine.tasks().independently().push(WORK, "order-1", "after").id();
        long delayed = engine.tasks().independently().push(WORK, Insertion.APPEND, Creation.of("order-2", "delayed").withDelay(Duration.ofHours(1))).id();
        tester.awaitIdle();
        assertThat(ran).as("the retrying task holds back the next one for its identifier").containsExactly("failing");

        assertThat(engine.admin().cancel("work", TaskQuery.ALL.inStates(QueueState.ACTIVE))).as("asks only for running tasks").isZero();
        assertThat(engine.admin().cancel("work", List.of(failing, delayed, 999_999L))).isEqualTo(2);
        tester.awaitIdle();

        assertThat(ran).as("the held-back task ran once the one before it was cancelled").containsExactly("failing", "after");
        assertThat(engine.admin().find(held).orElseThrow().state()).isEqualTo("succeeded");
        TaskView cancelled = engine.admin().find(failing).orElseThrow();
        assertThat(cancelled.state()).isEqualTo("cancelled");
        assertThat(cancelled.finished()).isTrue();
        assertThat(cancelled.attempt()).as("the attempt it took").isEqualTo(1);
        assertThat(engine.admin().find(delayed).orElseThrow().state()).isEqualTo("cancelled");
        Instant later = tester.advance(Duration.ofHours(2));
        tester.awaitIdle();
        assertThat(ran).as("neither the retry nor the delayed task ran").containsExactly("failing", "after");
        assertThat(engine.admin().purge("work", Outcome.CANCELLED, later)).as("kept until purged, like any finished task").isEqualTo(2);
        assertThatThrownBy(() -> engine.admin().cancel("missing", TaskQuery.ALL)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("A delayed or retrying task runs once an operator runs it now, with the attempts it took")
    void waiting_tasks_run_now() throws Exception {
        List<String> ran = new CopyOnWriteArrayList<>();
        Set<String> failedOnce = ConcurrentHashMap.newKeySet();
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(ManualClock.startingNow()))
            .configure("work-module", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                ran.add(task.payload());
                return task.payload().equals("flaky") && failedOnce.add(task.payload()) ? Decision.failed("not yet") : Decision.success();
            })))
            .build()
            .start();
        TaskTester tester = TaskTester.of(engine);
        long flaky = engine.tasks().independently().push(WORK, "order-1", "flaky").id();
        long delayed = engine.tasks().independently().push(WORK, Insertion.APPEND, Creation.of("order-2", "delayed").withDelay(Duration.ofHours(1))).id();
        long untouched = engine.tasks().independently().push(WORK, Insertion.APPEND, Creation.of("order-3", "untouched").withDelay(Duration.ofHours(1))).id();
        tester.awaitIdle();
        assertThat(ran).containsExactly("flaky");
        assertThat(engine.admin().find(flaky).orElseThrow().state()).isEqualTo("retrying");

        assertThat(engine.admin().runNow("work", TaskQuery.ALL.inStates(QueueState.READY, QueueState.SUSPENDED))).as("asks for no waiting task").isZero();
        assertThat(engine.admin().runNow("work", List.of(flaky, delayed, 999_999L))).isEqualTo(2);
        tester.awaitIdle();

        assertThat(ran).containsExactlyInAnyOrder("flaky", "flaky", "delayed");
        TaskView retried = engine.admin().find(flaky).orElseThrow();
        assertThat(retried.state()).isEqualTo("succeeded");
        assertThat(retried.attempt()).as("the attempt that failed, and this one").isEqualTo(2);
        assertThat(engine.admin().find(delayed).orElseThrow().state()).isEqualTo("succeeded");
        assertThat(engine.admin().find(untouched).orElseThrow().state()).isEqualTo("scheduled");
        assertThatThrownBy(() -> engine.admin().runNow("missing", TaskQuery.ALL)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("A page limit below one is refused before the store sees it")
    void a_page_limit_below_one_is_refused() throws Exception {
        start(new InMemoryTaskStore());
        assertThatThrownBy(() -> engine.admin().deadLetters("work", 0, 0)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("limit");
        assertThatThrownBy(() -> engine.admin().tasks("work", TaskQuery.ALL, -1)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("limit");
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
    @DisplayName("Topic status: every topic with its module and whether this node runs it")
    void topic_status_lists_every_topic() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        start(store);
        try (StoreTransaction transaction = store.begin()) {
            store.createTopic(transaction, "elsewhere");
            transaction.commit();
        }

        assertThat(engine.admin().topics()).extracting(TopicStatus::name, TopicStatus::processing, TopicStatus::module)
            .containsExactly(
                Tuple.tuple("elsewhere", false, null),
                Tuple.tuple("work", true, "work-module"));
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
    @DisplayName("Unfinished tasks are found; a run time waits, one in the past doesn't")
    void unfinished_tasks_are_found() throws Exception {
        ManualClock clock = new ManualClock(Instant.parse("2026-11-21T10:00:00Z"));
        TopicRef<String> next = TopicRef.of("next", String.class);
        Set<String> ran = ConcurrentHashMap.newKeySet();
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .configure("work", tasks -> tasks
                .topic(WORK, batch -> batch.decide(task -> {
                    ran.add(task.payload());
                    if (task.payload().equals("chain")) {
                        task.push(next, Insertion.APPEND, Creation.of("chained", "chained").withRunAt(clock.instant().plus(Duration.ofHours(2))));
                    }
                    return Decision.success();
                }))
                .topic(next, batch -> batch.decide(task -> {
                    ran.add(task.payload());
                    return Decision.success();
                })))
            .build()
            .start();
        TaskTester tester = TaskTester.of(engine);
        long tomorrow = engine.tasks().independently().push(WORK, Insertion.APPEND,
            Creation.of("a", "tomorrow").withRunAt(clock.instant().plus(Duration.ofDays(1)))).id();
        long later = engine.tasks().independently().push(WORK, Insertion.APPEND, Creation.of("b", "later").withDelay(Duration.ofHours(1))).id();
        engine.tasks().independently().push(WORK, Insertion.APPEND, Creation.of("c", "overdue").withRunAt(clock.instant().minusSeconds(60)));
        engine.tasks().independently().push(WORK, "d", "chain");
        tester.awaitIdle();
        assertThat(ran).as("a run time in the past runs at once").containsExactlyInAnyOrder("overdue", "chain");
        assertThat(tester.task(tomorrow).runAt()).isEqualTo(clock.instant().plus(Duration.ofDays(1)));

        assertThat(engine.admin().tasks("work", TaskQuery.ALL, 10)).extracting(TaskView::id).containsExactly(tomorrow, later);
        assertThat(engine.admin().tasks("next", TaskQuery.ALL.inStates(QueueState.SCHEDULED), 10)).singleElement()
            .satisfies(task -> assertThat(task.runAt()).as("downstream work keeps its run time").isEqualTo(clock.instant().plus(Duration.ofHours(2))));

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
                .eachTask(retired, task -> { }))
                .build().start();
             TaskEngine other = TaskEngine.builder().store(store).configure("work", tasks -> tasks
                .eachTask(retired, task -> { }))
                .build().start()) {
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
            .configure("work", tasks -> tasks.eachTask(WORK, task -> { })).build().prepare()) {
            engine.tasks().independently().push(WORK, "a", "waiting");

            assertThatThrownBy(() -> engine.admin().deleteTopic("work")).hasMessageContaining("has 1 unfinished task(s)");
            assertThat(engine.admin().topic("work")).isPresent();
        }
    }
}
