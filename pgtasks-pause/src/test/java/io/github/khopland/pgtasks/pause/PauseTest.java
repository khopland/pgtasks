package io.github.khopland.pgtasks.pause;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskEngineBuilder;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.engine.extension.ExtensionContext;
import io.github.khopland.pgtasks.engine.extension.OperatorView;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.ManualClock;
import io.github.khopland.pgtasks.spi.StoreTransaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class PauseTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);
    private static final Duration SOON = Duration.ofSeconds(2);
    /** Long enough for a claim that shouldn't happen to happen: topics poll every 100 ms or less here. */
    private static final Duration A_WHILE = Duration.ofMillis(300);

    private final InMemoryTaskStore store = new InMemoryTaskStore();
    private final Set<String> processed = ConcurrentHashMap.newKeySet();
    private final List<TaskEngine> engines = new ArrayList<>();

    @AfterEach
    void stop() {
        engines.forEach(TaskEngine::close);
    }

    private TaskEngine node(String name, Pause pause) {
        return node(name, pause, builder -> builder);
    }

    private TaskEngine node(String name, Pause pause, UnaryOperator<TaskEngineBuilder> more) {
        TaskEngine engine = more.apply(TaskEngine.builder().store(store).node(name).extension(pause)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> processed.add(task.payload()),
                options -> options.pollInterval(Duration.ofMillis(50))))).build().start();
        engines.add(engine);
        return engine;
    }

    /** A node that runs no topics, as an operator's would. */
    private TaskEngine operator(Pause pause, UnaryOperator<TaskEngineBuilder> more) {
        TaskEngine engine = more.apply(TaskEngine.builder().store(store).node("operator").extension(pause)).build().start();
        engines.add(engine);
        return engine;
    }

    private static void push(TaskEngine engine, String payload) {
        engine.tasks().independently().push(WORK, payload, payload);
    }

    @Test
    void a_paused_topic_takes_pushes_but_no_node_claims_until_it_is_resumed() throws Exception {
        Pause onA = new Pause();
        Pause onB = new Pause();
        TaskEngine nodeA = node("node-a", onA);
        TaskEngine nodeB = node("node-b", onB);
        push(nodeA, "before");
        await("the first task to run", () -> processed.contains("before"));

        assertThat(onA.pause("work")).isTrue();
        assertThat(onA.pause("work")).as("paused already").isFalse();
        assertThat(onB.isPaused("work")).as("every node reads the same pauses").isTrue();
        assertThat(onB.paused()).extracting(PauseStore.Paused::topic).containsExactly("work");
        push(nodeB, "while-paused");
        push(nodeA, "also-while-paused");
        Thread.sleep(A_WHILE.toMillis());
        assertThat(processed).containsExactly("before");

        assertThat(onB.resume("work")).isTrue();
        assertThat(onB.resume("work")).as("not paused any more").isFalse();
        // A paused topic waits 30 s between its gate's answers: only the resume's wake-up runs these this soon.
        await("the waiting tasks to run", () -> processed.containsAll(Set.of("while-paused", "also-while-paused")));
        assertThat(onA.paused()).isEmpty();
    }

    @Test
    void a_pause_outlasts_a_restart_and_holds_a_node_that_starts_later() throws Exception {
        Pause first = new Pause();
        TaskEngine nodeA = node("node-a", first);
        first.pause("work");
        push(nodeA, "waiting");
        nodeA.close();

        Pause second = new Pause();
        node("node-b", second);
        Thread.sleep(A_WHILE.toMillis());
        assertThat(processed).as("the new node read the pause before its first claim").isEmpty();

        second.resume("work");
        await("the waiting task to run", () -> processed.contains("waiting"));
    }

    @Test
    void a_stopped_and_restarted_engine_still_holds_a_paused_topic() throws Exception {
        Pause pause = new Pause();
        TaskEngine engine = node("node-a", pause);
        pause.pause("work");
        engine.stop(Duration.ofSeconds(5));
        push(engine, "waiting");
        engine.start();
        Thread.sleep(A_WHILE.toMillis());
        assertThat(processed).isEmpty();

        pause.resume("work");
        await("the waiting task to run", () -> processed.contains("waiting"));
    }

    @Test
    void without_notifications_the_heartbeat_carries_a_pause_and_a_resume() throws Exception {
        Pause operator = new Pause();
        operator(operator, builder -> builder.notifications(false));
        TaskEngine worker = node("worker", new Pause(),
            builder -> builder.notifications(false).heartbeat(Duration.ofMillis(100), Duration.ofSeconds(5)));

        operator.pause("work");
        // The worker hears nothing; its next heartbeat reloads the pauses.
        Thread.sleep(A_WHILE.toMillis());
        push(worker, "waiting");
        Thread.sleep(A_WHILE.toMillis());
        assertThat(processed).isEmpty();

        operator.resume("work");
        await("a heartbeat to pick the resume up and wake the topic", () -> processed.contains("waiting"));
    }

    @Test
    void only_existing_topics_are_paused_and_only_an_installed_extension_acts() {
        Pause pause = new Pause();
        assertThatThrownBy(() -> pause.pause("work")).isInstanceOf(IllegalStateException.class).hasMessageContaining("isn't installed");

        node("node-a", pause);
        assertThatThrownBy(() -> pause.pause("missing")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("missing");
        assertThat(pause.resume("missing")).isFalse();
        assertThat(pause.paused()).isEmpty();
    }

    @Test
    void other_topics_run_while_one_is_paused() {
        TopicRef<String> other = TopicRef.of("other", String.class);
        Pause pause = new Pause();
        TaskEngine engine = TaskEngine.builder().store(store).extension(pause)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> processed.add("work/" + task.payload()))
                .eachTask(other, task -> processed.add("other/" + task.payload())))
            .build().start();
        engines.add(engine);
        pause.pause("work");

        engine.tasks().independently().push(other, "a", "a");
        await("the other topic's task to run", () -> processed.contains("other/a"));
        assertThat(processed).containsExactly("other/a");
    }

    @Test
    void a_node_with_the_extension_forgets_the_pauses_of_deleted_topics() {
        ManualClock clock = ManualClock.startingNow();
        InMemoryTaskStore clocked = new InMemoryTaskStore(clock);
        Pause pause = new Pause();
        TaskEngine engine = TaskEngine.builder().store(clocked).extension(pause)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> { })).build().start();
        engines.add(engine);
        pause.pause("work");
        engine.close();
        engine.admin().deleteTopic("work");
        assertThat(pause.paused()).extracting(PauseStore.Paused::topic).containsExactly("work");

        TaskEngine next = TaskEngine.builder().store(clocked).extension(new Pause()).build().start();
        engines.add(next);
        TaskTester.of(next).advance(Pause.CLEANUP_INTERVAL);
        assertThat(pause.paused()).isEmpty();
    }

    @Test
    void a_pause_store_that_cannot_be_readied_fails_the_start() {
        PauseStore broken = new PauseStore() {
            @Override
            public void prepare() {
                throw new IllegalStateException("no table");
            }

            @Override
            public boolean pause(StoreTransaction transaction, String topic) {
                return false;
            }

            @Override
            public boolean resume(StoreTransaction transaction, String topic) {
                return false;
            }

            @Override
            public List<Paused> list(StoreTransaction transaction) {
                return List.of();
            }
        };
        TaskEngine engine = TaskEngine.builder().store(store).extension(new Pause(broken)).build();
        engines.add(engine);
        assertThatThrownBy(engine::start).hasMessage("no table");
    }

    @Test
    void the_in_memory_store_has_one_pause_store_per_task_store() {
        InMemoryPauseCapability capability = new InMemoryPauseCapability();
        assertThat(capability.create(PauseStore.class, store)).containsSame(capability.create(PauseStore.class, store).orElseThrow());
        assertThat(capability.create(PauseStore.class, new InMemoryTaskStore())).isNotEqualTo(capability.create(PauseStore.class, store));
        assertThat(capability.create(ExtensionContext.class, store)).isEmpty();
    }

    @Test
    void operators_pause_and_resume_through_the_view() throws Exception {
        Pause pause = new Pause();
        TaskEngine engine = node("a", pause);
        OperatorView view = engine.operatorViews().stream().filter(each -> each.name().equals("pause")).findFirst().orElseThrow();
        assertThat(view.topic("work")).isEqualTo(OperatorView.TopicState.of(null, "pause"));
        assertThat(view.rows()).isEmpty();

        perform(view, "pause", "work");
        assertThat(pause.isPaused("work")).isTrue();
        assertThat(view.topic("work")).isEqualTo(OperatorView.TopicState.of("paused", "resume"));
        assertThat(view.rows()).singleElement().satisfies(row -> {
            assertThat(row.key()).isEqualTo("work");
            assertThat(row.values()).containsKeys("topic", "since");
            assertThat(row.actions()).containsExactly("resume");
        });
        push(engine, "held");
        await("the health to show the paused topic", () -> engine.heldTopics().stream()
            .anyMatch(held -> held.gate().equals("Pause") && "paused".equals(held.reason())));

        perform(view, "resume", "work");
        await("the resumed topic's task", () -> processed.contains("held"));
        assertThat(view.rows()).isEmpty();
    }

    private static void perform(OperatorView view, String action, String key) throws Exception {
        view.actions().stream().filter(each -> each.name().equals(action)).findFirst().orElseThrow().perform().perform(key);
    }

    @Test
    void an_in_memory_pause_rolls_back_with_its_transaction() throws Exception {
        PauseStore pauses = new InMemoryPauseCapability().create(PauseStore.class, store).orElseThrow();
        try (StoreTransaction transaction = store.begin()) {
            assertThat(pauses.pause(transaction, "work")).isTrue();
            transaction.rollback();
        }
        try (StoreTransaction transaction = store.begin()) {
            assertThat(pauses.list(transaction)).as("undone with the transaction").isEmpty();
            transaction.commit();
        }
    }

    private static void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + SOON.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting for " + what);
            }
            Thread.onSpinWait();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }
}
