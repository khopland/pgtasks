package org.example.task2.engine;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.example.task2.Ordering;
import org.example.task2.spi.TaskStore;

/**
 * The poll loop of one topic on this node. Takes a topic permit and a node permit, claims at
 * most a batch (and at most what the rate limit admits), and runs the batch on its own virtual
 * thread. Waits for a wake-up or the poll interval when the topic is empty, and backs off when
 * polling fails (finding #3).
 */
final class TopicWorker<T> {

    private static final System.Logger LOGGER = System.getLogger(TopicWorker.class.getName());

    private static final Duration FIRST_BACKOFF = Duration.ofMillis(100);

    /** How long past a stop's deadline threads get to react to being cancelled. */
    static final Duration PATIENCE = Duration.ofSeconds(5);

    private final Topic<T> topic;
    private final int topicId;
    private final EngineCore core;
    private final Semaphore permits;
    private final Signal signal = new Signal();
    private final Signal limitSignal = new Signal();
    private final Signal stopSignal = new Signal();
    private final ExecutorService batches;
    private volatile boolean running, stopping;
    private Thread poller;

    // For telling when the engine is idle: claims started, the last one that found nothing, batches running.
    private final AtomicLong claims = new AtomicLong();
    private final AtomicLong claimedTasks = new AtomicLong();
    private final AtomicInteger runningBatches = new AtomicInteger();
    private volatile long lastEmptyClaim = -1;
    /** The wake-ups there had been when the last empty claim began. */
    private volatile long signalsAtEmptyClaim;

    TopicWorker(Topic<T> topic, int topicId, EngineCore core) {
        this.topic = topic;
        this.topicId = topicId;
        this.core = core;
        this.permits = new Semaphore(topic.settings().concurrency());
        this.batches = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("task2-" + topic.ref().name() + "-", 0).factory());
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
        poller = Thread.ofVirtual().name("task2-poll-" + topic.ref().name()).start(this::loop);
    }

    void wake() {
        signal.signal();
    }

    /** The topic's rate limit changed: a wait for permits ends, and the next claim uses the new values. */
    void limitChanged() {
        limitSignal.signal();
    }

    /** The claim, with the rate limit's current values: an operator can change them at any time. */
    private TaskStore.ClaimRequest request() {
        RateLimit limit = topic.settings().rateLimit() == null ? null : core.limits().effective(topic.settings().rateLimit());
        return new TaskStore.ClaimRequest(
            topicId,
            core.nodeId(),
            topic.settings().ordering(),
            topic.settings().batchSize(),
            limit == null ? null : new TaskStore.Admission(limit.name(), limit.perSecond(), limit.burst())
        );
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
            try {
                TaskStore.ClaimRequest request = request();
                TaskStore.Claim claimed = core.transact(transaction -> core.store().claim(transaction, request));
                failures = 0;
                if (!claimed.tasks().isEmpty()) {
                    claimed.tasks().forEach(task -> core.held().put(task.id(), task.attempt()));
                    BatchRun<T> run = new BatchRun<>(this, core, claimed.tasks());
                    claimedTasks.addAndGet(claimed.tasks().size());
                    runningBatches.incrementAndGet();
                    try {
                        batches.execute(() -> {
                            try {
                                run.run();
                            } catch (RuntimeException | Error e) {
                                LOGGER.log(System.Logger.Level.ERROR, "A batch in " + topic.ref() + " failed unexpectedly", e);
                            } finally {
                                runningBatches.decrementAndGet();
                                release();
                                // Finishing a task can unblock later tasks for its identifier. No
                                // row becomes newly ready then, so no notification is sent: poll again.
                                if (topic.settings().ordering() != Ordering.FIFO) {
                                    signal.signal();
                                }
                            }
                        });
                    } catch (RejectedExecutionException e) {
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
                release();
                dispatched = true;
                if (claimed.admissionWait() != null) {
                    // New work doesn't shorten a wait for the rate limit; a changed limit does.
                    limitSignal.await(claimed.admissionWait());
                } else {
                    signalsAtEmptyClaim = signalsBefore;
                    lastEmptyClaim = claim;
                    signal.await(topic.settings().pollInterval());
                }
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                if (!dispatched) {
                    release();
                    dispatched = true;
                }
                if (!running) {
                    return;
                }
                failures++;
                Duration backoff = backoff(failures);
                LOGGER.log(System.Logger.Level.WARNING, "Polling " + topic.ref() + " failed " + failures + " time(s) in a row, retrying in " + backoff, e);
                try {
                    stopSignal.await(backoff);
                } catch (InterruptedException interrupted) {
                    return;
                }
            } finally {
                if (!dispatched) {
                    release();
                }
            }
        }
    }

    long claims() {
        return claims.get();
    }

    long claimedTasks() {
        return claimedTasks.get();
    }

    /**
     * No batch is running, a claim started after {@code mark} found nothing to do, and nothing has
     * happened since: no claim started and no wake-up arrived. Without the last part, work that
     * another topic's completion released just after this claim (a fan-in, say) went unnoticed.
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
        limitSignal.signal();
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
                LOGGER.log(System.Logger.Level.WARNING, "Batches in " + topic.ref() + " ignored cancellation and are still running");
            }
        }
        // Past the deadline: a poller still waiting, for permits or the store, is interrupted.
        if (current != null && current.isAlive()) {
            current.interrupt();
            if (!current.join(PATIENCE)) {
                LOGGER.log(System.Logger.Level.WARNING, "Polling " + topic.ref() + " ignored the stop and is still running");
            }
        }
    }
}
