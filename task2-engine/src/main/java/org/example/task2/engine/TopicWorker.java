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

    private final Topic<T> topic;
    private final int topicId;
    private final EngineCore core;
    private final Semaphore permits;
    private final Signal signal = new Signal();
    private final ExecutorService batches;
    private volatile boolean running, stopping;
    private Thread poller;

    // For telling when the engine is idle: claims started, the last one that found nothing, batches running.
    private final AtomicLong claims = new AtomicLong();
    private final AtomicLong claimedTasks = new AtomicLong();
    private final AtomicInteger runningBatches = new AtomicInteger();
    private volatile long lastEmptyClaim = -1;

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

    private void loop() {
        TaskStore.ClaimRequest request = new TaskStore.ClaimRequest(
            topicId,
            core.nodeId(),
            topic.settings().ordering(),
            topic.settings().batchSize(),
            topic.settings().rateLimit() == null ? null : new TaskStore.Admission(
                topic.settings().rateLimit().name(),
                topic.settings().rateLimit().perSecond(),
                topic.settings().rateLimit().burst()
            )
        );
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
            boolean dispatched = false;
            long claim = claims.incrementAndGet();
            try {
                TaskStore.Claim claimed = core.transact(transaction -> core.store().claim(transaction, request));
                failures = 0;
                if (!claimed.tasks().isEmpty()) {
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
                        // Stopping: hand the tasks back instead of leaving them until expiry.
                        runningBatches.decrementAndGet();
                        run.releaseAll();
                        throw e;
                    }
                    dispatched = true;
                    continue;
                }
                release();
                dispatched = true;
                if (claimed.admissionWait() != null) {
                    // A wake-up doesn't shorten a wait for the rate limit.
                    Signal.sleep(claimed.admissionWait());
                } else {
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
                    Signal.sleep(backoff);
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

    /** No batch is running, and a claim started after {@code mark} found nothing to do. */
    boolean quietSince(long mark) {
        return runningBatches.get() == 0 && lastEmptyClaim > mark;
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

    /** Stops polling; batches already running carry on. */
    void stopPolling() {
        running = false;
        Thread current = poller;
        if (current != null) {
            current.interrupt();
        }
        batches.shutdown();
    }

    /** Waits for running batches until the deadline, then cancels what is left. */
    void awaitBatches(long deadlineNanos) throws InterruptedException {
        Thread current = poller;
        if (current != null) {
            current.join(Duration.ofNanos(Math.max(0, deadlineNanos - System.nanoTime())));
        }
        if (!batches.awaitTermination(Math.max(0, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS)) {
            stopping = true;
            batches.shutdownNow();
            if (!batches.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.log(System.Logger.Level.WARNING, "Batches in " + topic.ref() + " ignored cancellation");
            }
        }
    }
}
