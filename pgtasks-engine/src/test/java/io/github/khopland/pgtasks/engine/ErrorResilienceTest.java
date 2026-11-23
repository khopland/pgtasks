package io.github.khopland.pgtasks.engine;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Review 2026-11-23: background loops caught Exception only. An Error from the store (here a
 * linkage error, as a stale jar on the class path gives) cancelled the scheduled heartbeat for
 * good, or ended a topic's poll thread, without a word.
 */
class ErrorResilienceTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    private final MemoryEngines engines = new MemoryEngines();
    private final List<String> ran = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        engines.close();
    }

    /** The memory store, except that the first call of {@code method} throws an Error. */
    private TaskStore failingOnce(String method) {
        AtomicBoolean failed = new AtomicBoolean();
        return (TaskStore) Proxy.newProxyInstance(TaskStore.class.getClassLoader(), new Class<?>[] {TaskStore.class},
            (proxy, called, args) -> {
                if (called.getName().equals(method) && failed.compareAndSet(false, true)) {
                    throw new AbstractMethodError("stale " + method);
                }
                try {
                    return called.invoke(engines.store, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
    }

    private TaskEngine start(TaskStore store) {
        List<Topic<?>> topics = List.of(Topic.of(WORK, batch -> batch.decide(task -> {
            ran.add(task.identifier());
            return Decision.success();
        })));
        return engines.add(new TaskEngine(store, MemoryEngines.settings("node-a").withNotifier(engines.store.notifier()), topics)).start();
    }

    @Test
    void a_heartbeat_that_throws_an_error_beats_again() throws Exception {
        TaskEngine engine = start(failingOnce("heartbeat"));

        // The lease is taken after the heartbeat, so only a heartbeat that ran again takes it.
        EngineFixture.await("a heartbeat after the error", Duration.ofSeconds(5), engine::leadsHousekeeping);
    }

    @Test
    void a_claim_that_throws_an_error_is_retried_and_the_topic_keeps_polling() throws Exception {
        TaskEngine engine = start(failingOnce("claim"));

        engine.tasks().independently().push(WORK, "after", "payload");

        EngineFixture.await("the task ran", Duration.ofSeconds(5), () -> !ran.isEmpty());
        assertThat(ran).containsExactly("after");
    }
}
