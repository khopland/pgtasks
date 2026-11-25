package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.engine.extension.ClaimGate;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The poll loop of one topic on this node. Takes a topic permit and a node permit, claims at
 * most a batch, and runs the batch on its own virtual
 * thread. Waits for a wake-up or the poll interval when the topic is empty, and backs off when
 * polling fails (finding #3).
 */
final class TopicWorker<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicWorker.class);

    private static final Duration FIRST_BACKOFF = Duration.ofMillis(100);

    /** How long past a stop's deadline threads get to react to being cancelled. */
    static final Duration PATIENCE = Duration.ofSeconds(5);

    private final Topic<T> topic;
    private final int topicId;
    private final EngineCore core;
    private final Semaphore permits;
    private final Signal signal = new Signal();
    private final Signal stopSignal = new Signal();
    private final ExecutorService batches;
    private volatile boolean running, stopping;
    private @Nullable Thread poller;

    // For telling when the engine is idle: claims started, the last one that found nothing, batches running.
    private final AtomicLong claims = new AtomicLong();
    private final AtomicLong claimedTasks = new AtomicLong();
    private final AtomicInteger runningBatches = new AtomicInteger();
    /** The batches running now, so a fence can cancel them. */
    private final Set<BatchRun<T>> inProgress = ConcurrentHashMap.newKeySet();
    private volatile long lastEmptyClaim = -1;
    /** The wake-ups there had been when the last empty claim began. */
    private volatile long signalsAtEmptyClaim;

    TopicWorker(Topic<T> topic, int topicId, EngineCore core) {
        this.topic = topic;
        this.topicId = topicId;
        this.core = core;
        this.permits = new Semaphore(topic.settings().concurrency());
        this.batches = Executors.newThreadPerTaskExecutor(core.threads().get().name("pgtasks-" + topic.ref().name() + "-", 0).factory());
    }

    Topic<T> topic() {
        return topic;
    }

    int topicId() {
        return topicId;
    }

    boolean stopping() {
        return stopping;
    }

    void start() {
        running = true;
        poller = core.threads().get().name("pgtasks-poll-" + topic.ref().name()).start(this::loop);
    }

    void wake() {
        signal.signal();
    }

    private TaskStore.ClaimRequest request(int size) {
        return new TaskStore.ClaimRequest(topicId, core.nodeId(), topic.settings().ordering(), size);
    }

    private void loop() {
        int failures = 0;
        while (running) {
            try {
                permits.acquire();
                try {
                    core.inFlight().acquire();
                } catch (InterruptedException e) {
                    permits.release();
                    throw e;
                }
            } catch (InterruptedException e) {
                return;
            }
            if (!running) {
                release();
                return;
            }
            boolean dispatched = false;
            long signalsBefore = signal.count();
            long claim = claims.incrementAndGet();
            int limit = topic.settings().batchSize();
            Duration gateWait = Duration.ZERO;
            boolean fenced = core.fenced().getAsBoolean();
            if (fenced) {
                // The heartbeat is too late: claims could be taken over by another node while they run.
                // Counts as having found nothing; lifting the fence wakes this worker. The gates aren't
                // asked, so none hands out a permit for a claim that isn't made.
                gateWait = topic.settings().pollInterval();
            }
            for (ClaimGate gate : fenced ? List.<ClaimGate>of() : core.gates()) {
                ClaimGate.Permit permit;
                try {
                    permit = gate.permit(topic.ref());
                } catch (RuntimeException e) {
                    // A broken gate doesn't stop the topic: it claims as if the gate allowed it.
                    LOGGER.warn("A claim gate failed for {}; claiming without it", topic.ref(), e);
                    permit = ClaimGate.Permit.ANY;
                }
                if (permit.waits()) {
                    gateWait = permit.delay().compareTo(gateWait) > 0 ? permit.delay() : gateWait;
                } else {
                    limit = Math.min(limit, permit.maxTasks());
                }
            }
            if (!gateWait.isZero()) {
                // Held back by a gate: counts as having found nothing, so an idle node looks idle.
                release();
                signalsAtEmptyClaim = signalsBefore;
                lastEmptyClaim = claim;
                try {
                    signal.await(gateWait.compareTo(topic.settings().pollInterval()) > 0 ? topic.settings().pollInterval() : gateWait);
                } catch (InterruptedException e) {
                    return;
                }
                continue;
            }
            // The node-wide permit covers talking to the store, not the handler, which holds no
            // connection: a topic whose handlers hang can't starve the node's other topics.
            boolean nodeHeld = true;
            try {
                TaskStore.ClaimRequest request = request(limit);
                TaskStore.Claim claimed;
                try {
                    claimed = core.transact(transaction -> core.store().claim(transaction, request));
                } finally {
                    core.inFlight().release();
                    nodeHeld = false;
                }
                failures = 0;
                for (ClaimGate gate : core.gates()) {
                    try {
                        gate.claimed(topic.ref(), claimed.tasks().size());
                    } catch (RuntimeException e) {
                        LOGGER.warn("A claim gate failed for {}", topic.ref(), e);
                    }
                }
                if (!claimed.tasks().isEmpty()) {
                    claimed.tasks().forEach(task -> core.held().put(task.id(), task.attempt()));
                    BatchRun<T> run = new BatchRun<>(this, core, claimed.tasks());
                    claimedTasks.addAndGet(claimed.tasks().size());
                    runningBatches.incrementAndGet();
                    try {
                        inProgress.add(run);
                        batches.execute(() -> {
                            try {
                                run.run();
                            } catch (RuntimeException | Error e) {
                                LOGGER.error("A batch in {} failed unexpectedly", topic.ref(), e);
                            } finally {
                                inProgress.remove(run);
                                runningBatches.decrementAndGet();
                                permits.release();
                                // Finishing a task can unblock later tasks for its identifier. No
                                // row becomes newly ready then, so no notification is sent: poll again.
                                if (topic.settings().ordering() != Ordering.FIFO) {
                                    signal.signal();
                                }
                            }
                        });
                    } catch (RejectedExecutionException e) {
                        inProgress.remove(run);
                        // Stopping: hand the tasks back instead of leaving them until expiry. A stop
                        // past its deadline interrupts this thread, and a pool refuses an interrupted
                        // thread a connection, so the interrupt waits until they are back.
                        runningBatches.decrementAndGet();
                        boolean interrupted = Thread.interrupted();
                        try {
                            run.releaseAll();
                        } finally {
                            if (interrupted) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        throw e;
                    }
                    dispatched = true;
                    continue;
                }
                permits.release();
                dispatched = true;
                signalsAtEmptyClaim = signalsBefore;
                lastEmptyClaim = claim;
                signal.await(topic.settings().pollInterval());
            } catch (InterruptedException e) {
                return;
            } catch (Exception | Error e) {
                // An Error too (a linkage error, a stack overflow): it would end this thread, and the
                // topic would stop being polled on this node without a word.
                if (!dispatched) {
                    if (nodeHeld) {
                        core.inFlight().release();
                    }
                    permits.release();
                    dispatched = true;
                }
                if (!running) {
                    return;
                }
                failures++;
                Duration backoff = backoff(failures);
                LOGGER.warn("Polling {} failed {} time(s) in a row, retrying in {}", topic.ref(), failures, backoff, e);
                try {
                    stopSignal.await(backoff);
                } catch (InterruptedException interrupted) {
                    return;
                }
            } finally {
                if (!dispatched) {
                    // The node-wide permit only if this claim still holds it: an interrupt out of the
                    // claim comes after it was handed back, and releasing it twice would raise the cap.
                    if (nodeHeld) {
                        core.inFlight().release();
                    }
                    permits.release();
                }
            }
        }
    }

    /** Cancels the handlers of the batches running now: the node's heartbeat is too late. See {@link HeartbeatFence}. */
    void fence() {
        inProgress.forEach(BatchRun::fence);
    }

    long claims() {
        return claims.get();
    }

    long claimedTasks() {
        return claimedTasks.get();
    }

    /** The topic permits not taken by a claim or a running batch. */
    int freePermits() {
        return permits.availablePermits();
    }

    /**
     * No batch is running, a claim started after {@code mark} found nothing to do, and nothing has
     * happened since: no claim started and no wake-up arrived. Without the last part, work that
     * another topic's completion released just after this claim went unnoticed.
     */
    boolean quietSince(long mark) {
        long empty = lastEmptyClaim;
        return runningBatches.get() == 0 && empty > mark && empty == claims.get() && signal.count() == signalsAtEmptyClaim;
    }

    private Duration backoff(int failures) {
        long ceiling = topic.settings().pollInterval().toMillis();
        long millis = Math.min(ceiling, FIRST_BACKOFF.toMillis() << Math.min(failures - 1, 20));
        return Duration.ofMillis(millis / 2 + (long) (core.random().nextDouble() * millis / 2));
    }

    private void release() {
        core.inFlight().release();
        permits.release();
    }

    /**
     * Stops polling; batches already running carry on. The poller isn't interrupted, only woken:
     * interrupting a virtual thread in a socket read closes the socket, and a claim whose commit is
     * cut off that way may be committed without this node knowing. It stops at its next wait, or
     * once its claim returns, handing back what that claim got.
     */
    void stopPolling() {
        running = false;
        signal.signal();
        stopSignal.signal();
        batches.shutdown();
    }

    /**
     * Waits for running batches until the deadline, then cancels what is left: a cancelled batch
     * hands its tasks back without counting the attempt. Returns once the poller and every batch
     * thread have ended, or have ignored cancellation for {@link #PATIENCE} past the deadline.
     */
    void awaitBatches(long deadlineNanos) throws InterruptedException {
        Thread current = poller;
        if (current != null) {
            current.join(Duration.ofNanos(Math.max(0, deadlineNanos - System.nanoTime())));
        }
        if (!batches.awaitTermination(Math.max(0, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS)) {
            stopping = true;
            batches.shutdownNow();
            if (!batches.awaitTermination(PATIENCE.toNanos(), TimeUnit.NANOSECONDS)) {
                LOGGER.warn("Batches in {} ignored cancellation and are still running", topic.ref());
            }
        }
        // Past the deadline: a poller still waiting, for permits or the store, is interrupted.
        if (current != null && current.isAlive()) {
            current.interrupt();
            if (!current.join(PATIENCE)) {
                LOGGER.warn("Polling {} ignored the stop and is still running", topic.ref());
            }
        }
    }
}
