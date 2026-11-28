package io.github.khopland.pgtasks.engine;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.random.RandomGenerator;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.TaskQuery;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One {@link TopicWorker} on the in-memory store under Fray's scheduler, stopped while it claims
 * and runs batches: whatever the interleaving, the stop hands back every permit, the node's and
 * the topic's, and leaves no task claimed. Run with {@code -Pfray}.
 */
@ExtendWith(FrayTestExtension.class)
class TopicWorkerFrayTest {

    private static final TopicRef<String> TOPIC = TopicRef.of("fray", String.class);

    @FrayTest(iterations = 300)
    void a_stop_racing_a_claim_hands_back_every_permit_and_claim() throws Exception {
        assertStopLeavesNothingBehind(1, 1, 1, false, false);
    }

    @FrayTest(iterations = 300)
    void a_stop_racing_a_failed_claim_and_its_backoff_hands_back_every_permit_and_claim() throws Exception {
        assertStopLeavesNothingBehind(1, 1, 1, true, true);
    }

    @FrayTest(iterations = 300)
    void a_stop_racing_two_batches_and_a_wake_up_hands_back_every_permit_and_claim() throws Exception {
        assertStopLeavesNothingBehind(2, 2, 1, true, false);
    }

    /** @param failFirstClaim whether the first claim fails on a cut connection, so the worker backs off */
    private static void assertStopLeavesNothingBehind(int tasks, int concurrency, int nodeCap, boolean wake, boolean failFirstClaim)
        throws Exception {
        TaskStore store = failFirstClaim ? failingFirstClaim(new InMemoryTaskStore()) : new InMemoryTaskStore();
        int topicId = EngineCore.transact(store, transaction -> store.createTopic(transaction, TOPIC.name()));
        int nodeId = EngineCore.transact(store, transaction -> store.registerNode(transaction, "fray", null));
        EngineCore.transact(store, transaction -> {
            for (int task = 0; task < tasks; task++) {
                store.push(transaction, topicId, Insertion.APPEND,
                    List.of(new TaskStore.NewTask("task-" + task, "payload", null, null, null, false, 0, null, null, null, Map.of())));
            }
            return null;
        });

        Topic<String> topic = Topic.of(TOPIC, batch -> batch.decide(task -> Decision.success()))
            .with(TopicSettings.defaults().withConcurrency(concurrency).withBatchSize(1).withPollInterval(Duration.ofMinutes(1)));
        TaskEngine engine = new TaskEngine(store, EngineSettings.defaults("fray"), List.of(topic));
        Semaphore inFlight = new Semaphore(nodeCap);
        Map<Long, Integer> held = new ConcurrentHashMap<>();
        TopicWorker<String> worker = new TopicWorker<>(topic, topicId, new EngineCore(store, (DefaultTasks) engine.tasks(), () -> nodeId, inFlight,
            RandomGenerator.getDefault(), name -> { }, held, () -> false, TaskObserver.NONE, List.of(), List.of(), List.of(), () -> false, Thread::ofPlatform));

        worker.start();
        Thread waker = Thread.ofPlatform().start(() -> {
            if (wake) {
                worker.wake();
            }
        });
        Thread stopper = Thread.ofPlatform().start(() -> {
            worker.stopPolling();
            try {
                // Short: when a bug leaves a thread blocked, Fray waits this out in real time.
                worker.awaitBatches(System.nanoTime() + Duration.ofSeconds(5).toNanos());
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        });
        waker.join();
        stopper.join();

        assertThat(inFlight.availablePermits()).as("node permits after stop").isEqualTo(nodeCap);
        assertThat(worker.freePermits()).as("topic permits after stop").isEqualTo(concurrency);
        assertThat(held).as("claims this node still holds").isEmpty();
        List<TaskView> left = EngineCore.transact(store, transaction -> store.tasks(transaction, topicId, TaskQuery.ALL, 10));
        assertThat(left).as("tasks still claimed").noneMatch(task -> !task.finished() && task.nodeId() != null);
    }

    private static TaskStore failingFirstClaim(TaskStore store) {
        AtomicBoolean failed = new AtomicBoolean();
        return (TaskStore) Proxy.newProxyInstance(TaskStore.class.getClassLoader(), new Class<?>[] {TaskStore.class}, (proxy, method, arguments) -> {
            if (method.getName().equals("claim") && failed.compareAndSet(false, true)) {
                throw new SQLTransientConnectionException("cut connection (test)");
            }
            try {
                return method.invoke(store, arguments);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }
}
