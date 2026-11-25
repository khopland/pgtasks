package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

import io.github.khopland.pgtasks.Outcome;
import io.github.khopland.pgtasks.engine.extension.HousekeepingJob;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(Housekeeper.class);
    private static final int BATCH = 1_000;
    /**
     * A task whose node dies under it waits 30 s, then 60 s, before it runs again, and the third
     * time it is dead-lettered whatever its retry policy says, even forever(): a task that crashes
     * the JVM would otherwise go from node to node, taking each one down.
     */
    static final TaskStore.Expiries EXPIRIES = new TaskStore.Expiries(3, Duration.ofSeconds(30));

    /** What the node does when its heartbeat is so late that it fences itself, and when that ends; see {@link HeartbeatFence}. */
    interface Fencing {
        void fenced();

        void lifted();
    }

    /** Kept for the engine's own callbacks. */
    @FunctionalInterface
    interface Job extends HousekeepingJob.Action {
    }

    private final TaskStore store;
    private final EngineSettings settings;
    private final IntSupplier nodeId;
    private final Job rejoin;
    private final IntConsumer wake;
    private final Job sweepOrphans;
    private final Job reconcile;
    private final HeartbeatFence fence;
    private final Fencing fencing;
    /** What the watchdog last saw; only its thread touches it. */
    private boolean fenced;
    /** Jobs extensions added; each runs on a thread of its own. */
    private final List<HousekeepingJob> extensionJobs;
    private final Object leadership = new Object();
    private final Object beating = new Object();
    private @Nullable Instant leaseEnds;
    private volatile @Nullable Instant lastHeartbeat;
    /**
     * Four threads, so a slow job can't hold up the others: the heartbeat and lease alone on
     * one (a node that misses them loses the lease, and its health turns DOWN), the regular jobs
     * on another, retention, which can run long after downtime, on a third, and the fence's
     * watchdog, which never waits for the store, on a fourth. Each extension job gets one more, so
     * an extension can't hold up the engine's own.
     */
    private final List<ScheduledExecutorService> executors = new ArrayList<>();
    /** How long one retention run may delete before it leaves the rest to the next run. */
    static final Duration PURGE_BUDGET = Duration.ofSeconds(30);
    private final Duration purgeBudget;
    /** The topic a retention run ran out of time in; the next run starts with the one after it. */
    private @Nullable Integer purgeStoppedAt;

    /**
     * @param nodeId       this node's id now
     * @param rejoin       registers this node again, under a new id, once the cluster has forgotten it
     * @param wake         wakes this node's worker for a topic id
     * @param sweepOrphans releases tasks this node holds but isn't processing
     * @param reconcile    brings this node in line with the store, in case a notification was
     *                     lost: the topics it runs
     * @param fence        tells when the heartbeat is too late; its clock started when the node registered
     * @param fencing      stops this node's work while it is fenced, and resumes it after
     */
    Housekeeper(TaskStore store, EngineSettings settings, IntSupplier nodeId, Job rejoin, IntConsumer wake, Job sweepOrphans, Job reconcile,
                Duration purgeBudget, List<HousekeepingJob> extensionJobs, HeartbeatFence fence, Fencing fencing) {
        this.fence = fence;
        this.fencing = fencing;
        this.extensionJobs = List.copyOf(extensionJobs);
        this.purgeBudget = purgeBudget;
        this.store = store;
        this.settings = settings;
        this.nodeId = nodeId;
        this.rejoin = rejoin;
        this.wake = wake;
        this.sweepOrphans = sweepOrphans;
        this.reconcile = reconcile;
    }

    void start() {
        // Registering the node set its first heartbeat.
        lastHeartbeat = store.clock().instant();
        ScheduledExecutorService beats = executor("pgtasks-heartbeat-");
        ScheduledExecutorService jobs = executor("pgtasks-housekeeping-");
        ScheduledExecutorService slow = executor("pgtasks-retention-");
        schedule(beats, HousekeepingJob.everyNode("heartbeat", settings.heartbeat(), this::heartbeat));
        // Its own thread, which never waits for the store: it must act exactly when the heartbeat can't.
        schedule(executor("pgtasks-fence-"), HousekeepingJob.everyNode("fence", fence.checkInterval(), this::watch));
        for (HousekeepingJob job : coreJobs()) {
            schedule(job.name().equals(RETENTION) ? slow : jobs, job);
        }
        for (HousekeepingJob job : extensionJobs) {
            schedule(executor("pgtasks-job-" + job.name() + "-"), job);
        }
    }

    private static final String RETENTION = "retention";

    /** The engine's own jobs, besides the heartbeat. */
    private List<HousekeepingJob> coreJobs() {
        return List.of(
            HousekeepingJob.everyNode("orphan sweep", settings.heartbeat(), sweepOrphans),
            HousekeepingJob.everyNode("reconcile", settings.heartbeat(), reconcile),
            HousekeepingJob.leader("expiry", settings.heartbeat(), this::expire),
            HousekeepingJob.leader("promotion", settings.promoteInterval(), this::promote),
            HousekeepingJob.leader(RETENTION, settings.purgeInterval(), this::purge),
            HousekeepingJob.leader("count snapshot", settings.snapshotInterval(), this::snapshot));
    }

    private ScheduledExecutorService executor(String name) {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, Thread.ofVirtual().name(name, 0).factory());
        executors.add(executor);
        return executor;
    }

    @SuppressWarnings("FutureReturnValueIgnored") // the job logs its own failures; stop() shuts the executor down
    private void schedule(ScheduledExecutorService executor, HousekeepingJob job) {
        String name = job.name();
        Duration interval = job.interval();
        executor.scheduleWithFixedDelay(() -> {
            try {
                if (job.scope() == HousekeepingJob.Scope.EVERY_NODE || leading()) {
                    job.run();
                }
            } catch (Exception e) {
                LOGGER.warn("The {} job failed; it runs again in {}", name, interval, e);
            } catch (Error e) {
                // Caught too: anything thrown out of a scheduled job cancels it for good, without a word,
                // and a node whose heartbeat stopped that way would be taken for dead while still running.
                LOGGER.error("The {} job failed with an error; it runs again in {}", name, interval, e);
            }
        }, 0, interval.toNanos(), TimeUnit.NANOSECONDS);
    }

    /** Stops the jobs and gives the lease up, so another node can take over at once. */
    void stop() {
        // Lets a job in progress finish: interrupting it would close its connection mid-statement.
        executors.forEach(ScheduledExecutorService::shutdown);
        try {
            for (ScheduledExecutorService executor : executors) {
                if (!executor.awaitTermination(TopicWorker.PATIENCE.toNanos(), TimeUnit.NANOSECONDS)) {
                    executor.shutdownNow();
                    if (!executor.awaitTermination(TopicWorker.PATIENCE.toNanos(), TimeUnit.NANOSECONDS)) {
                        LOGGER.warn("A housekeeping job ignored the stop and is still running");
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executors.clear();
        boolean held;
        synchronized (leadership) {
            held = leaseEnds != null;
            leaseEnds = null;
        }
        if (held) {
            try {
                EngineCore.transact(store, transaction -> {
                    store.resign(transaction, LEASE, nodeId.getAsInt());
                    return null;
                });
            } catch (Exception e) {
                LOGGER.warn("Could not give up the housekeeping lease; another node takes over when its term runs out", e);
            }
        }
    }

    /** When this node last recorded its heartbeat in the store. */
    @Nullable Instant lastHeartbeat() {
        return lastHeartbeat;
    }

    /** Whether this node holds the housekeeping lease now, by the store's clock. */
    boolean leading() {
        synchronized (leadership) {
            return leaseEnds != null && store.clock().instant().isBefore(leaseEnds);
        }
    }

    /**
     * Heartbeats, then renews or takes the lease. A node silent for {@link #FORGET_NODES_AFTER}
     * without claims is forgotten, but its process may still be alive, after a suspended VM or a
     * long partition: it finds its row gone, and registers again under a new id rather than fail
     * every heartbeat and claim from then on. It held no claims, and no lease (deleted with it).
     */
    void heartbeat() throws Exception {
        // One at a time, so a heartbeat asked for (heartbeatNow) and the scheduled one can't both register again.
        synchronized (beating) {
            beat();
        }
    }

    private void beat() throws Exception {
        long began = fence.now();
        Instant beat = store.clock().instant();
        int node = nodeId.getAsInt();
        if (!EngineCore.transact(store, transaction -> store.heartbeat(transaction, node))) {
            synchronized (leadership) {
                leaseEnds = null;
            }
            rejoin.run();
        }
        fence.beat(began);
        lastHeartbeat = beat;
        Duration term = settings.leaseTerm();
        // Counted from before the call, so this node never believes in a lease longer than the store does.
        Instant asked = store.clock().instant();
        int leader = nodeId.getAsInt();
        boolean held = EngineCore.transact(store, transaction -> store.lead(transaction, LEASE, leader, term));
        boolean was;
        synchronized (leadership) {
            was = leaseEnds != null && asked.isBefore(leaseEnds);
            leaseEnds = held ? asked.plus(term) : null;
        }
        if (held != was) {
            LOGGER.info(held ? "This node now runs the cluster-wide housekeeping" : "Another node now runs the cluster-wide housekeeping");
        }
    }

    /** Fences this node when its heartbeat is too late, and lifts the fence once one gets through. */
    private void watch() {
        boolean now = fence.fenced();
        if (now == fenced) {
            return;
        }
        fenced = now;
        if (now) {
            LOGGER.error("No heartbeat got through for {}; other nodes take over this node's tasks once it has been silent for {}. "
                + "It stops claiming and cancels its running handlers until a heartbeat gets through", fence.silence(), settings.expiry());
            fencing.fenced();
        } else {
            LOGGER.warn("A heartbeat got through again; this node claims again");
            fencing.lifted();
        }
    }

    /** If this node leads, runs the cluster-wide jobs once, now, extensions' included: for tests that move the clock. */
    void runLeaderJobsNow() throws Exception {
        if (leading()) {
            for (HousekeepingJob job : coreJobs()) {
                if (job.scope() == HousekeepingJob.Scope.LEADER) {
                    job.run();
                }
            }
            for (HousekeepingJob job : extensionJobs) {
                if (job.scope() == HousekeepingJob.Scope.LEADER) {
                    job.run();
                }
            }
        }
    }

    /** Nodes silent for this long, holding no claims, are forgotten. */
    static final Duration FORGET_NODES_AFTER = Duration.ofDays(3);

    private void expire() throws Exception {
        EngineCore.transact(store, transaction -> store.requeueExpired(transaction, nodeId.getAsInt(), settings.expiry(), settings.expiries())).forEach(wake::accept);
        EngineCore.transact(store, transaction -> store.deleteDeadNodes(transaction, FORGET_NODES_AFTER));
    }

    /** Promotes everything due, a batch per transaction, for as long as batches come back full. */
    private void promote() throws Exception {
        TaskStore.Promoted promoted;
        do {
            promoted = EngineCore.transact(store, transaction -> store.promoteDue(transaction, BATCH));
            promoted.topics().forEach(wake::accept);
        } while (promoted.tasks() == BATCH && leading());
    }

    /**
     * Deletes finished tasks past their retention, counted from completion, a batch at a time,
     * for at most {@link #PURGE_BUDGET}: after downtime, or after retention was shortened, the rest
     * waits for the next run rather than holding the job. A run that runs out of time leaves the
     * next run to start with the following topic, so a topic with a large backlog can't keep the
     * others from ever being purged. A topic whose retention fails for another reason than the
     * store being out of reach is logged and passed over, so it can't stop the topics after it.
     */
    private void purge() throws Exception {
        Instant now = store.clock().instant();
        long deadline = System.nanoTime() + purgeBudget.toNanos();
        Map<Integer, Map<Outcome, Period>> retention = new TreeMap<>(EngineCore.transact(store, store::retention));
        List<Integer> order = new ArrayList<>(retention.keySet());
        Integer stoppedAt = purgeStoppedAt;
        if (stoppedAt != null) {
            int start = (int) order.stream().filter(id -> id <= stoppedAt).count();
            Collections.rotate(order, -start);
        }
        purgeStoppedAt = null;
        for (int topic : order) {
            for (Map.Entry<Outcome, Period> kept : Objects.requireNonNull(retention.get(topic)).entrySet()) {
                try {
                    if (!purge(topic, kept.getKey(), kept.getValue(), now, deadline)) {
                        purgeStoppedAt = topic;
                        LOGGER.info("Retention used its {} for this run; it goes on in {}", purgeBudget, settings.purgeInterval());
                        return;
                    }
                } catch (Exception e) {
                    if (Transient.is(e)) {
                        throw e;
                    }
                    LOGGER.warn("Could not apply the retention of {} ({}) to topic {}; the other topics go on", kept.getKey().label(), kept.getValue(), topic, e);
                }
            }
        }
    }

    /** Deletes one outcome's expired tasks of a topic; false once the run's time is up. */
    private boolean purge(int topic, Outcome outcome, Period period, Instant now, long deadline) throws Exception {
        Instant cutoff = now.atZone(ZoneOffset.UTC).minus(period).toInstant();
        int deleted;
        do {
            deleted = EngineCore.transact(store, transaction -> store.purge(transaction, topic, outcome, cutoff, BATCH));
            if (System.nanoTime() > deadline) {
                return false;
            }
        } while (deleted == BATCH && leading());
        return true;
    }

    private void snapshot() throws Exception {
        EngineCore.transact(store, transaction -> {
            store.snapshotCounts(transaction);
            return null;
        });
    }

    @Override
    public String toString() {
        return "Housekeeper[node " + nodeId.getAsInt() + (leading() ? ", leading" : "") + "]";
    }
}
