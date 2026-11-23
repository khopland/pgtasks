package io.github.khopland.pgtasks.engine;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.ManualClock;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Dead letters that come back by themselves: on a new application version, or every so often. */
class DeadLetterPolicyTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    private final ManualClock clock = ManualClock.startingNow();
    private final InMemoryTaskStore store = new InMemoryTaskStore(clock);
    private final AtomicBoolean fixed = new AtomicBoolean();
    private final AtomicInteger runs = new AtomicInteger();
    private final List<TaskEngine> engines = new CopyOnWriteArrayList<>();

    @AfterEach
    void stop() {
        engines.forEach(TaskEngine::close);
    }

    /** The store, except that a revive fails while {@link #reviveFails} says so. */
    private final AtomicBoolean reviveFails = new AtomicBoolean();
    private final TaskStore flaky = (TaskStore) Proxy.newProxyInstance(TaskStore.class.getClassLoader(), new Class<?>[] {TaskStore.class},
        (proxy, called, args) -> {
            if (called.getName().equals("revive") && reviveFails.get()) {
                throw new SQLException("revive failed");
            }
            try {
                return called.invoke(store, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });

    private TaskEngine start(String version, DeadLetterPolicy policy) throws Exception {
        TaskEngine engine = TaskEngine.builder()
            .store(flaky)
            .globals(globals -> globals.heartbeat(Duration.ofMillis(50), Duration.ofHours(1)))
            .version(version)
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                runs.incrementAndGet();
                return fixed.get() ? Decision.success() : Decision.failed("bug");
            }), topic -> topic.retry(Retry.none()).deadLetters(policy)))
            .build()
            .start();
        engines.add(engine);
        return engine;
    }

    @Test
    @DisplayName("The first node of a new version revives the dead letters once")
    void a_new_version_revives_the_dead_letters_once() throws Exception {
        DeadLetterPolicy policy = DeadLetterPolicy.KEEP.withReviveOnNewVersion();
        TaskEngine first = start("1.0", policy);
        long id = first.tasks().independently().push(WORK, "a", "payload").id();
        TaskTester.of(first).awaitIdle();
        assertThat(first.admin().find(id).orElseThrow().state()).isEqualTo("failed");
        first.close();

        TaskEngine restarted = start("1.0", policy);
        TaskTester.of(restarted).awaitIdle();
        assertThat(restarted.admin().find(id).orElseThrow().state()).as("the same version doesn't revive again").isEqualTo("failed");
        restarted.close();

        fixed.set(true);
        TaskEngine upgraded = start("1.1", policy);
        TaskEngine sameVersion = start("1.1", policy);
        TaskTester.of(upgraded, sameVersion).awaitIdle();

        assertThat(upgraded.admin().find(id).orElseThrow().state()).isEqualTo("succeeded");
        assertThat(runs).as("one failure, one run after the revival").hasValue(2);
    }

    /** Review 2026-11-23: the version was claimed before the revival, so a failed revival was never retried. */
    @Test
    @DisplayName("A revival for a new version that fails is retried, not recorded as done")
    void a_failed_revival_for_a_new_version_is_retried() throws Exception {
        DeadLetterPolicy policy = DeadLetterPolicy.KEEP.withReviveOnNewVersion();
        TaskEngine first = start("1.0", policy);
        long id = first.tasks().independently().push(WORK, "a", "payload").id();
        TaskTester.of(first).awaitIdle();
        first.close();

        fixed.set(true);
        reviveFails.set(true);
        TaskEngine upgraded = start("1.1", policy);
        assertThat(upgraded.admin().find(id).orElseThrow().state()).as("the revival failed").isEqualTo("failed");
        Thread.sleep(200);
        assertThat(upgraded.admin().find(id).orElseThrow().state()).as("and is tried again").isEqualTo("failed");

        reviveFails.set(false);
        EngineFixture.await("the retried revival", Duration.ofSeconds(5),
            () -> upgraded.admin().find(id).map(task -> task.state().equals("succeeded")).orElse(true));
        assertThat(runs).hasValue(2);
    }

    @Test
    @DisplayName("A revival for a new version that a node didn't finish is left to the next node of the version")
    void a_revival_rolled_back_is_left_to_the_next_node() throws Exception {
        DeadLetterPolicy policy = DeadLetterPolicy.KEEP.withReviveOnNewVersion();
        TaskEngine first = start("1.0", policy);
        long id = first.tasks().independently().push(WORK, "a", "payload").id();
        TaskTester.of(first).awaitIdle();
        first.close();

        fixed.set(true);
        reviveFails.set(true);
        start("1.1", policy).close();
        reviveFails.set(false);
        TaskEngine next = start("1.1", policy);
        TaskTester.of(next).awaitIdle();

        assertThat(next.admin().find(id).orElseThrow().state()).isEqualTo("succeeded");
    }

    @Test
    @DisplayName("A sweep revives the dead letters every so often, like the 1.x reset job")
    void a_sweep_revives_them_every_so_often() throws Exception {
        TaskEngine engine = start("1.0", DeadLetterPolicy.KEEP.withReviveEvery(Duration.ofHours(6)));
        TaskTester tester = TaskTester.of(engine);
        long id = engine.tasks().independently().push(WORK, "a", "payload").id();
        tester.awaitIdle();
        assertThat(engine.admin().find(id).orElseThrow().state()).isEqualTo("failed");

        tester.advance(Duration.ofHours(1));
        tester.awaitIdle();
        assertThat(engine.admin().find(id).orElseThrow().state()).as("not yet").isEqualTo("failed");

        fixed.set(true);
        tester.advance(Duration.ofHours(6));
        tester.awaitIdle();
        assertThat(engine.admin().find(id).orElseThrow().state()).isEqualTo("succeeded");
    }

    @Test
    @DisplayName("A sweep that fails is tried again at the next heartbeat, not skipped for a period")
    void a_failed_sweep_is_retried() throws Exception {
        TaskEngine engine = start("1.0", DeadLetterPolicy.KEEP.withReviveEvery(Duration.ofHours(6)));
        TaskTester tester = TaskTester.of(engine);
        long id = engine.tasks().independently().push(WORK, "a", "payload").id();
        tester.awaitIdle();

        fixed.set(true);
        reviveFails.set(true);
        tester.advance(Duration.ofHours(7));
        Thread.sleep(200);
        assertThat(engine.admin().find(id).orElseThrow().state()).isEqualTo("failed");

        reviveFails.set(false);
        EngineFixture.await("the retried sweep", Duration.ofSeconds(5),
            () -> engine.admin().find(id).map(task -> task.state().equals("succeeded")).orElse(true));
    }
}
