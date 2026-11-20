package org.example.task2.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

import org.example.task2.Outcome;
import org.example.task2.spi.TaskStore;

/**
 * Housekeeping on one node. Every node heartbeats, renews or takes the housekeeping lease, and
 * releases claims it lost track of. The node holding the lease also runs the cluster-wide jobs:
 * requeueing tasks of dead nodes, promoting due tasks, deleting finished tasks past their
 * retention, and snapshotting queue counts. Retention is read from the store, where every node
 * records it for its topics, so the leader applies it to topics it doesn't run itself.
 *
 * <p>The lease lasts four heartbeats unless renewed, so a leader that dies or hangs is replaced
 * by the next node that asks. The jobs are safe to run on two nodes at once, which can happen
 * for at most one heartbeat while a hung leader wakes up and finds its lease gone.
 */
final class Housekeeper {

    static final String LEASE = "housekeeping";

    private static final System.Logger LOGGER = System.getLogger(Housekeeper.class.getName());
    private static final int BATCH = 1_000;

    @FunctionalInterface
    interface Job {
        void run() throws Exception;
    }

    private final TaskStore store;
    private final EngineSettings settings;
    private final int nodeId;
    private final IntConsumer wake;
    private final Job sweepOrphans;
    private final Object leadership = new Object();
    private Instant leaseEnds;
    private ScheduledExecutorService executor;

    /**
     * @param wake         wakes this node's worker for a topic id
     * @param sweepOrphans releases tasks this node holds but isn't processing
     */
    Housekeeper(TaskStore store, EngineSettings settings, int nodeId, IntConsumer wake, Job sweepOrphans) {
        this.store = store;
        this.settings = settings;
        this.nodeId = nodeId;
        this.wake = wake;
        this.sweepOrphans = sweepOrphans;
    }

    void start() {
        executor = Executors.newScheduledThreadPool(1, Thread.ofVirtual().name("task2-housekeeping-", 0).factory());
        schedule("heartbeat", settings.heartbeat(), this::heartbeat);
        schedule("orphan sweep", settings.heartbeat(), sweepOrphans);
        schedule("expiry", settings.heartbeat(), () -> {
            if (leading()) {
                expire();
            }
        });
        schedule("promotion", settings.promoteInterval(), () -> {
            if (leading()) {
                promote();
            }
        });
        schedule("retention", settings.purgeInterval(), () -> {
            if (leading()) {
                purge();
            }
        });
        schedule("count snapshot", settings.snapshotInterval(), () -> {
            if (leading()) {
                snapshot();
            }
        });
    }

    private void schedule(String name, Duration interval, Job job) {
        executor.scheduleWithFixedDelay(() -> {
            try {
                job.run();
            } catch (Exception e) {
                LOGGER.log(System.Logger.Level.WARNING, "The " + name + " job failed; it runs again in " + interval, e);
            }
        }, 0, interval.toNanos(), TimeUnit.NANOSECONDS);
    }

    /** Stops the jobs and gives the lease up, so another node can take over at once. */
    void stop() {
        if (executor != null) {
            executor.shutdownNow();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        boolean held;
        synchronized (leadership) {
            held = leaseEnds != null;
            leaseEnds = null;
        }
        if (held) {
            try {
                EngineCore.transact(store, transaction -> {
                    store.resign(transaction, LEASE, nodeId);
                    return null;
                });
            } catch (Exception e) {
                LOGGER.log(System.Logger.Level.WARNING, "Could not give up the housekeeping lease; another node takes over when its term runs out", e);
            }
        }
    }

    /** Whether this node holds the housekeeping lease now, by the store's clock. */
    boolean leading() {
        synchronized (leadership) {
            return leaseEnds != null && store.clock().instant().isBefore(leaseEnds);
        }
    }

    /** Heartbeats, then renews or takes the lease. */
    void heartbeat() throws Exception {
        EngineCore.transact(store, transaction -> {
            store.heartbeat(transaction, nodeId);
            return null;
        });
        Duration term = settings.leaseTerm();
        // Counted from before the call, so this node never believes in a lease longer than the store does.
        Instant asked = store.clock().instant();
        boolean held = EngineCore.transact(store, transaction -> store.lead(transaction, LEASE, nodeId, term));
        boolean was;
        synchronized (leadership) {
            was = leaseEnds != null && asked.isBefore(leaseEnds);
            leaseEnds = held ? asked.plus(term) : null;
        }
        if (held != was) {
            LOGGER.log(System.Logger.Level.INFO, held ? "This node now runs the cluster-wide housekeeping" : "Another node now runs the cluster-wide housekeeping");
        }
    }

    /** If this node leads, runs the cluster-wide jobs once, now: for tests that move the clock. */
    void runLeaderJobsNow() throws Exception {
        if (leading()) {
            expire();
            promote();
            purge();
            snapshot();
        }
    }

    private void expire() throws Exception {
        EngineCore.transact(store, transaction -> store.requeueExpired(transaction, nodeId, settings.expiry())).forEach(wake::accept);
    }

    private void promote() throws Exception {
        List<Integer> promoted;
        do {
            promoted = EngineCore.transact(store, transaction -> store.promoteDue(transaction, BATCH));
            promoted.forEach(wake::accept);
        } while (promoted.size() == BATCH && leading());
    }

    /** Deletes finished tasks past their retention, counted from completion, a batch at a time. */
    private void purge() throws Exception {
        Instant now = store.clock().instant();
        Map<Integer, Map<Outcome, Period>> retention = EngineCore.transact(store, store::retention);
        for (Map.Entry<Integer, Map<Outcome, Period>> topic : retention.entrySet()) {
            for (Map.Entry<Outcome, Period> kept : topic.getValue().entrySet()) {
                Instant cutoff = now.atZone(ZoneOffset.UTC).minus(kept.getValue()).toInstant();
                int deleted;
                do {
                    deleted = EngineCore.transact(store, transaction -> store.purge(transaction, topic.getKey(), kept.getKey(), cutoff, BATCH));
                } while (deleted == BATCH && leading());
            }
        }
    }

    private void snapshot() throws Exception {
        EngineCore.transact(store, transaction -> {
            store.snapshotCounts(transaction);
            return null;
        });
    }

    @Override
    public String toString() {
        return "Housekeeper[node " + nodeId + (leading() ? ", leading" : "") + "]";
    }
}
