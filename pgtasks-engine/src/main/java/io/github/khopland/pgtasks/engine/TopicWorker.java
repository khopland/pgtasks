package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
    /** The gate holding claims back now, why, since when, and until when it said, by the store's clock; null while claims go ahead. */
    private volatile @Nullable Held held;

    private record Held(String gate, @Nullable String reason, Instant since, Instant until) {
    }

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
            boolean probing = false;
            Duration gateWait = Duration.ZERO;
            boolean fenced = core.fenced().getAsBoolean();
            if (fenced) {
                // The heartbeat is too late: claims could be taken over by another node while they run.
                // Counts as having found nothing; lifting the fence wakes this worker. The gates aren't
                // asked, so none hands out a permit for a claim that isn't made.
                gateWait = topic.settings().pollInterval();
            }
            // The gates that granted a permit; each is told once what came of it.
            List<ClaimGate> granted = new ArrayList<>(fenced ? 0 : core.gates().size());
            ClaimGate holding = null;
            ClaimGate.Permit holdingPermit = null;
            for (ClaimGate gate : fenced ? List.<ClaimGate>of() : core.gates()) {
                ClaimGate.Permit permit;
                try {
                    permit = gate.permit(topic.ref(), limit);
                } catch (RuntimeException e) {
                    // A broken gate doesn't stop the topic: it claims as if the gate allowed it.
                    LOGGER.warn("A claim gate of {} failed for {}; claiming without it", NamedGate.nameOf(gate), topic.ref(), e);
                    continue;
                }
                if (permit.waits()) {
                    if (permit.delay().compareTo(gateWait) > 0) {
                        gateWait = permit.delay();
                        holding = gate;
                        holdingPermit = permit;
                    }
                } else {
                    granted.add(gate);
                    limit = Math.min(limit, permit.maxTasks());
                    probing |= permit.probing();
                }
            }
            if (holding != null && holdingPermit != null) {
                held(NamedGate.nameOf(holding), holdingPermit);
            } else if (!fenced) {
                held = null;
            }
            if (!gateWait.isZero()) {
                // Held back by a gate: counts as having found nothing, so an idle node looks idle.
                // The gates that did grant a permit get it back, unused.
                settle(granted, List.of());
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
                TaskStore.Claim claimed = null;
                try {
                    claimed = core.transact(transaction -> core.store().claim(transaction, request));
                } finally {
                    core.inFlight().release();
                    nodeHeld = false;
                    // A failed claim got nothing, so the gates get their permits back.
                    settle(granted, claimed == null ? List.of() : claimed.tasks().stream().map(TaskStore.ClaimedTask::id).toList());
                }
                failures = 0;
                if (!claimed.tasks().isEmpty()) {
                    claimed.tasks().forEach(task -> core.held().put(task.id(), task.attempt()));
                    BatchRun<T> run = new BatchRun<>(this, core, claimed.tasks(), probing);
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

    /** Tells each gate that granted this claim's permit which tasks it got. */
    private void settle(List<ClaimGate> granted, List<Long> taskIds) {
        for (ClaimGate gate : granted) {
            try {
                gate.claimed(topic.ref(), taskIds);
            } catch (RuntimeException e) {
                LOGGER.warn("A claim gate of {} failed for {}", NamedGate.nameOf(gate), topic.ref(), e);
            }
        }
    }

    /** A gate held this claim back: keeps why, since when and until when, for the health and tests, and tells the observers. */
    private void held(String gate, ClaimGate.Permit permit) {
        Held before = held;
        Instant now = core.clock().instant();
        held = new Held(gate, permit.reason(), before != null ? before.since() : now, now.plus(permit.delay()));
        core.observer().held(topic.ref(), gate, permit.reason(), permit.delay());
    }

    /**
     * Whether a claim gate held this topic back at its last poll, why, since when and until when;
     * null if that poll claimed. However short the hold: {@link TaskEngine#heldTopics()} leaves
     * the short ones out.
     */
    EngineHealth.@Nullable HeldTopic heldBack() {
        Held now = held;
        return now == null ? null : new EngineHealth.HeldTopic(topic.ref().name(), now.gate(), now.reason(), now.since(), now.until());
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
        awaitUntil(deadlineNanos);
        cancel();
        awaitCancelled(System.nanoTime() + PATIENCE.toNanos());
    }

    /** Waits for the poller and the running batches until the deadline; whatever is left runs on. */
    void awaitUntil(long deadlineNanos) throws InterruptedException {
        Thread current = poller;
        if (current != null) {
            current.join(Duration.ofNanos(Math.max(0, deadlineNanos - System.nanoTime())));
        }
        batches.awaitTermination(Math.max(0, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
    }

    /**
     * Cancels the batches still running, whose tasks go back without counting the attempt, and
     * interrupts a poller still waiting, for permits or the store. Doesn't wait: a node stopping
     * cancels every topic's work at once, then gives them all one {@link #PATIENCE} together,
     * rather than each in turn.
     */
    void cancel() {
        if (!batches.isTerminated()) {
            stopping = true;
            batches.shutdownNow();
        }
        Thread current = poller;
        if (current != null && current.isAlive()) {
            current.interrupt();
        }
    }

    /** Waits for cancelled work until the deadline, and logs what still runs. */
    void awaitCancelled(long deadlineNanos) throws InterruptedException {
        if (!batches.awaitTermination(Math.max(0, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS)) {
            LOGGER.warn("Batches in {} ignored cancellation and are still running", topic.ref());
        }
        Thread current = poller;
        if (current != null && !current.join(Duration.ofNanos(Math.max(0, deadlineNanos - System.nanoTime())))) {
            LOGGER.warn("Polling {} ignored the stop and is still running", topic.ref());
        }
    }
}
