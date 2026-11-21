package org.example.task2.engine;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.example.task2.Creation;
import org.example.task2.Insertion;
import org.example.task2.Pushed;
import org.example.task2.TopicRef;
import org.example.task2.admin.QueueCounts;
import org.example.task2.admin.TaskView;
import org.example.task2.spi.ManualClock;
import org.example.task2.spi.StoreTransaction;
import org.example.task2.spi.TaskStore;

/**
 * Test helpers for one or more engines that share a store: jump the clock, wait until nothing
 * is left to do, drain a topic, and capture committed pushes. Clock jumps need the in-memory
 * store with a {@link ManualClock}; the rest works on any store.
 */
public final class TaskTester {

    /** A committed push, as a test sees it. */
    public record Captured<T>(TopicRef<T> topic, Insertion insertion, Creation<T> creation, Pushed result) {
    }

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final List<TaskEngine> engines;
    private final TaskStore store;
    private final List<Captured<?>> captured = new CopyOnWriteArrayList<>();

    private TaskTester(List<TaskEngine> engines) {
        if (engines.isEmpty()) {
            throw new IllegalArgumentException("Give the tester at least one engine");
        }
        this.engines = List.copyOf(engines);
        this.store = engines.getFirst().store();
        for (TaskEngine engine : engines) {
            if (engine.store() != store) {
                throw new IllegalArgumentException("Every engine must share one store");
            }
            engine.defaultTasks().observe(this::capture);
        }
    }

    public static TaskTester of(TaskEngine... engines) {
        return new TaskTester(List.of(engines));
    }

    public static TaskTester of(List<TaskEngine> engines) {
        return new TaskTester(engines);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void capture(TopicRef<?> topic, Insertion insertion, List<? extends Creation<?>> creations, List<Pushed> results) {
        for (int index = 0; index < creations.size(); index++) {
            captured.add(new Captured(topic, insertion, creations.get(index), results.get(index)));
        }
    }

    /**
     * Moves the store's clock forward, then runs every engine's housekeeping at once: the engines
     * were alive all along, so they all heartbeat first; then the housekeeping leader requeues
     * tasks of dead nodes, promotes due tasks, applies retention and snapshots counts. Needs a
     * store with a {@link ManualClock}.
     */
    public Instant advance(Duration duration) {
        if (!(store.clock() instanceof ManualClock clock)) {
            throw new IllegalStateException("advance(...) needs a store with a ManualClock, such as new InMemoryTaskStore(ManualClock.startingNow())");
        }
        Instant now = clock.advance(duration);
        housekeep();
        return now;
    }

    public void awaitIdle() {
        awaitIdle(DEFAULT_TIMEOUT);
    }

    /**
     * Waits until no engine has anything left to do now: every topic polled after the last wake-up
     * and found nothing, no batch is running, and a whole round claimed nothing new. Doesn't move
     * the clock, so delayed tasks and retries stay where they are.
     */
    public void awaitIdle(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            long before = claimed();
            housekeep();
            Map<TopicWorker<?>, Long> marks = new HashMap<>();
            for (TaskEngine engine : engines) {
                for (TopicWorker<?> worker : engine.workers()) {
                    marks.put(worker, worker.claims());
                    worker.wake();
                }
            }
            while (!marks.entrySet().stream().allMatch(mark -> mark.getKey().quietSince(mark.getValue()))) {
                if (System.nanoTime() > deadline) {
                    List<String> busy = marks.entrySet().stream()
                        .filter(mark -> !mark.getKey().quietSince(mark.getValue()))
                        .map(mark -> mark.getKey().topic().ref().name())
                        .toList();
                    throw new IllegalStateException("Not idle after " + timeout + "; still busy: " + busy);
                }
                pause();
            }
            if (claimed() == before) {
                return;
            }
        }
    }

    /**
     * Runs a topic until nothing in it can run any more, jumping the clock past delays and
     * backoffs when the store has a {@link ManualClock}. Suspended and dead-lettered tasks stay.
     */
    public QueueCounts drain(TopicRef<?> topic) {
        return drain(topic, DEFAULT_TIMEOUT);
    }

    public QueueCounts drain(TopicRef<?> topic, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            awaitIdle(Duration.ofNanos(Math.max(1, deadline - System.nanoTime())));
            QueueCounts counts = counts(topic);
            // Ready tasks wait for an open breaker's next probe.
            Instant probe = counts.ready() == 0 ? null : engines.stream().flatMap(engine -> engine.workers().stream())
                .filter(worker -> worker.topic().ref().name().equals(topic.name()))
                .map(TopicWorker::nextProbe).filter(java.util.Objects::nonNull).min(Instant::compareTo).orElse(null);
            Instant next = counts.nextRunAt() == null ? probe : probe == null || counts.nextRunAt().isBefore(probe) ? counts.nextRunAt() : probe;
            if (next == null) {
                return counts;
            }
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("Could not drain " + topic + " within " + timeout + ": " + counts);
            }
            Duration until = Duration.between(store.clock().instant(), next);
            if (store.clock() instanceof ManualClock) {
                advance(until.isNegative() ? Duration.ZERO : until);
            } else if (until.isPositive()) {
                sleep(until);
            }
        }
    }

    /** The committed pushes to a topic, in order. */
    @SuppressWarnings("unchecked")
    public <T> List<Creation<T>> pushed(TopicRef<T> topic) {
        List<Creation<T>> pushed = new ArrayList<>();
        for (Captured<?> each : captured) {
            if (each.topic().name().equals(topic.name()) && each.result().status() != Pushed.Status.PENDING) {
                pushed.add((Creation<T>) each.creation());
            }
        }
        return pushed;
    }

    /** The payloads of the committed pushes to a topic, in order. */
    public <T> List<T> pushedPayloads(TopicRef<T> topic) {
        return pushed(topic).stream().map(Creation::payload).toList();
    }

    /** The identifiers of the committed pushes to a topic, in order. */
    public List<String> pushedIdentifiers(TopicRef<?> topic) {
        return pushed(topic).stream().map(Creation::identifier).toList();
    }

    /** Fails unless exactly this many committed pushes went to the topic, saying which did. */
    public void assertPushed(TopicRef<?> topic, int count) {
        List<String> identifiers = pushedIdentifiers(topic);
        if (identifiers.size() != count) {
            throw new AssertionError("Expected " + count + " push(es) to " + topic + ", but " + identifiers.size() + " committed"
                + (identifiers.isEmpty() ? "" : ", for identifiers " + identifiers));
        }
    }

    /** Fails if any committed push went to the topic. */
    public void assertNothingPushed(TopicRef<?> topic) {
        assertPushed(topic, 0);
    }

    /** The task, finished or not; fails if the store doesn't have it. */
    public TaskView task(long taskId) {
        return find(taskId).orElseThrow(() -> new AssertionError("No task " + taskId));
    }

    public List<Captured<?>> captured() {
        return List.copyOf(captured);
    }

    public void clearCaptured() {
        captured.clear();
    }

    public QueueCounts counts(TopicRef<?> topic) {
        return read(transaction -> {
            int id = engines.getFirst().defaultTasks().topicId(transaction, topic.name());
            return store.count(transaction, id);
        });
    }

    public Optional<TaskView> find(long taskId) {
        return read(transaction -> store.find(transaction, taskId));
    }

    @FunctionalInterface
    private interface Read<T> {
        T run(StoreTransaction transaction) throws SQLException;
    }

    private <T> T read(Read<T> read) {
        try {
            return EngineCore.transact(store, read::run);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private long claimed() {
        return engines.stream().flatMap(engine -> engine.workers().stream()).mapToLong(TopicWorker::claimedTasks).sum();
    }

    private void housekeep() {
        try {
            for (TaskEngine engine : engines) {
                engine.heartbeatNow();
            }
            for (TaskEngine engine : engines) {
                engine.runLeaderJobsNow();
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void pause() {
        sleep(Duration.ofMillis(2));
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted", e);
        }
    }
}
