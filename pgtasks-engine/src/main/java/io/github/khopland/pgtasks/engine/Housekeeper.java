package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
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
 * for at most one heartbeat while a hung leader wakes up and finds its lease gone. Each leader
 * job of an extension has a lease of its own, named after its extension and itself and renewed
 * with the heartbeat, so it runs among the nodes that have it, whichever node runs the engine's
 * housekeeping.
 */
final class Housekeeper {

    static final String LEASE = "housekeeping";

    /** The engine's own jobs: an extension's job can't take these names, which the health shows them by. */
    static final Set<String> RESERVED = Set.of("heartbeat", "fence", "orphan sweep", "reconcile", "expiry", "promotion", "retention",
        "count snapshot");

    /**
     * An extension's job, and the name of its lease if it is a leader job: its extension's name
     * and its own ({@code <extension>/<job>}), so two extensions' jobs of one name, in one
     * application or two that share the schema, don't share a lease.
     */
    record ExtensionJob(HousekeepingJob job, String lease) {

        static ExtensionJob of(String extension, HousekeepingJob job) {
            return new ExtensionJob(job, extension + "/" + job.name());
        }

        String name() {
            return job.name();
        }

        boolean leader() {
            return job.scope() == HousekeepingJob.Scope.LEADER;
        }
    }

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
    private final List<ExtensionJob> extensionJobs;
    private final Object leadership = new Object();
    private final Object beating = new Object();
    private @Nullable Instant leaseEnds;
    /** When this node's lease of each extension leader job ends, by job name; guarded by {@link #leadership}. */
    private final Map<String, Instant> jobLeases = new HashMap<>();
    /** How each job's runs went on this node, by name. */
    private final Map<String, Runs> runs = new ConcurrentHashMap<>();
    /** The jobs whose runs are kept, besides the heartbeat and the fence, with whether each runs here now. */
    private final List<Tracked> tracked = new CopyOnWriteArrayList<>();

    private record Tracked(HousekeepingJob job, BooleanSupplier runsHere) {
    }

    /** Each extension job's thread and one run of it, by name, for {@link #runSoon}. */
    private final Map<String, Soon> soon = new ConcurrentHashMap<>();

    private record Soon(ScheduledExecutorService executor, Runnable run) {
    }

    private record Runs(@Nullable Instant lastRun, @Nullable Instant lastFailure, @Nullable String lastError, int failuresInARow) {
        static final Runs NONE = new Runs(null, null, null, 0);
    }
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
                Duration purgeBudget, List<ExtensionJob> extensionJobs, HeartbeatFence fence, Fencing fencing) {
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
        schedule(beats, HousekeepingJob.everyNode("heartbeat", settings.heartbeat(), this::heartbeat), () -> true, false);
        // Its own thread, which never waits for the store: it must act exactly when the heartbeat can't.
        schedule(executor("pgtasks-fence-"), HousekeepingJob.everyNode("fence", fence.checkInterval(), this::watch), () -> true, false);
        for (HousekeepingJob job : coreJobs()) {
            schedule(job.name().equals(RETENTION) ? slow : jobs, job, () -> job.scope() == HousekeepingJob.Scope.EVERY_NODE || leading(), true);
        }
        for (ExtensionJob each : extensionJobs) {
            HousekeepingJob job = each.job();
            ScheduledExecutorService executor = executor("pgtasks-job-" + job.name() + "-");
            soon.put(job.name(), new Soon(executor, schedule(executor, job, () -> runsHere(job.name()), true)));
        }
    }

    /**
     * Runs an extension's job once more, soon, on its own thread after a run in progress, as if
     * its interval had passed, if it runs on this node: a leader job only while this node holds
     * its lease. Nothing once stopped.
     *
     * @return false for a leader job whose lease this node doesn't hold, so the caller can ask
     *         the node that does
     */
    boolean runSoon(String job) {
        Soon each = soon.get(job);
        if (each == null) {
            // Stopped, or not started yet: nothing runs.
            return true;
        }
        if (!runsHere(job)) {
            return false;
        }
        try {
            each.executor().execute(each.run());
        } catch (RejectedExecutionException e) {
            // Stopping: the job runs no more.
        }
        return true;
    }

    /**
     * Runs the leader job of this lease soon, if this node holds the lease: another node was asked
     * to run it, and told this one on the job channel.
     */
    void runSoonByLease(String lease) {
        for (ExtensionJob each : extensionJobs) {
            if (each.leader() && each.lease().equals(lease) && leads(each.name())) {
                runSoon(each.name());
            }
        }
    }

    /** Whether an extension's job runs on this node now: every-node jobs always, leader jobs while this node holds their lease. */
    boolean runsHere(String job) {
        for (ExtensionJob each : extensionJobs) {
            if (each.name().equals(job)) {
                return !each.leader() || leads(job);
            }
        }
        return false;
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

    /**
     * @param runsHere whether the job runs on this node now, asked before each run
     * @param track    whether its runs show in {@link #jobs()}
     */
    @SuppressWarnings("FutureReturnValueIgnored") // the job logs its own failures; stop() shuts the executor down
    private Runnable schedule(ScheduledExecutorService executor, HousekeepingJob job, BooleanSupplier runsHere, boolean track) {
        String name = job.name();
        Duration interval = job.interval();
        if (track) {
            tracked.add(new Tracked(job, runsHere));
        }
        Runnable run = () -> {
            Throwable failure = null;
            try {
                if (!runsHere.getAsBoolean()) {
                    return;
                }
                job.run();
            } catch (Exception e) {
                failure = e;
                LOGGER.warn("The {} job failed; it runs again in {}", name, interval, e);
            } catch (Error e) {
                // Caught too: anything thrown out of a scheduled job cancels it for good, without a word,
                // and a node whose heartbeat stopped that way would be taken for dead while still running.
                failure = e;
                LOGGER.error("The {} job failed with an error; it runs again in {}", name, interval, e);
            }
            if (track) {
                ran(name, failure);
            }
        };
        executor.scheduleWithFixedDelay(run, 0, interval.toNanos(), TimeUnit.NANOSECONDS);
        return run;
    }

    /** Records that a job's run ended, and how. Never throws: it runs in the job's own catch. */
    private void ran(String name, @Nullable Throwable failure) {
        try {
            Instant now = store.clock().instant();
            runs.compute(name, (ignored, before) -> {
                Runs last = before == null ? Runs.NONE : before;
                return failure == null
                    ? new Runs(now, last.lastFailure(), last.lastError(), 0)
                    : new Runs(now, now, String.valueOf(failure), last.failuresInARow() + 1);
            });
        } catch (RuntimeException e) {
            LOGGER.debug("Could not record the run of the {} job", name, e);
        }
    }

    /** The jobs this node has, the engine's and the extensions', with how their runs went here. */
    List<EngineHealth.JobStatus> jobs() {
        List<EngineHealth.JobStatus> jobs = new ArrayList<>();
        for (Tracked each : tracked) {
            HousekeepingJob job = each.job();
            Runs last = runs.getOrDefault(job.name(), Runs.NONE);
            jobs.add(new EngineHealth.JobStatus(job.name(), job.scope(), each.runsHere().getAsBoolean(), last.lastRun(), last.lastFailure(),
                last.lastError(), last.failuresInARow()));
        }
        return jobs;
    }

    /** Stops the jobs and gives the lease up, so another node can take over at once. */
    void stop() {
        // Lets a job in progress finish: interrupting it would close its connection mid-statement.
        executors.forEach(ScheduledExecutorService::shutdown);
        try {
            // One patience for all the jobs together, then one more after interrupting what is left.
            long patience = System.nanoTime() + TopicWorker.PATIENCE.toNanos();
            for (ScheduledExecutorService executor : executors) {
                executor.awaitTermination(Math.max(0, patience - System.nanoTime()), TimeUnit.NANOSECONDS);
            }
            List<ScheduledExecutorService> left = executors.stream().filter(executor -> !executor.isTerminated()).toList();
            left.forEach(ScheduledExecutorService::shutdownNow);
            long interrupted = System.nanoTime() + TopicWorker.PATIENCE.toNanos();
            for (ScheduledExecutorService executor : left) {
                if (!executor.awaitTermination(Math.max(0, interrupted - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                    LOGGER.warn("A housekeeping job ignored the stop and is still running");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executors.clear();
        soon.clear();
        List<String> held = new ArrayList<>();
        synchronized (leadership) {
            if (leaseEnds != null) {
                held.add(LEASE);
            }
            leaseEnds = null;
            extensionJobs.stream().filter(each -> jobLeases.containsKey(each.name())).map(ExtensionJob::lease).forEach(held::add);
            jobLeases.clear();
        }
        if (!held.isEmpty()) {
            try {
                EngineCore.transact(store, transaction -> {
                    for (String lease : held) {
                        store.resign(transaction, lease, nodeId.getAsInt());
                    }
                    return null;
                });
            } catch (Exception e) {
                LOGGER.warn("Could not give up the leases {}; other nodes take over when their terms run out", held, e);
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

    /** Whether this node holds the lease of an extension's leader job now, by the job's name and the store's clock. */
    boolean leads(String job) {
        synchronized (leadership) {
            Instant ends = jobLeases.get(job);
            return ends != null && store.clock().instant().isBefore(ends);
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
                jobLeases.clear();
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
        renewJobLeases(leader, term);
    }

    /**
     * Takes or renews the lease of each extension leader job, in one transaction. One that can't
     * be renewed runs out with its term, and the job stops running here when it does.
     */
    private void renewJobLeases(int leader, Duration term) {
        List<ExtensionJob> leaders = extensionJobs.stream().filter(ExtensionJob::leader).toList();
        if (leaders.isEmpty()) {
            return;
        }
        Instant asked = store.clock().instant();
        Map<String, Boolean> held;
        try {
            held = EngineCore.transact(store, transaction -> {
                Map<String, Boolean> leases = new LinkedHashMap<>();
                for (ExtensionJob each : leaders) {
                    leases.put(each.name(), store.lead(transaction, each.lease(), leader, term));
                }
                return leases;
            });
        } catch (Exception e) {
            LOGGER.warn("Could not renew the leases of the jobs {}; each stops running here once its term runs out",
                leaders.stream().map(ExtensionJob::name).toList(), e);
            return;
        }
        held.forEach((name, now) -> {
            boolean was;
            synchronized (leadership) {
                Instant ends = jobLeases.get(name);
                was = ends != null && asked.isBefore(ends);
                if (now) {
                    jobLeases.put(name, asked.plus(term));
                } else {
                    jobLeases.remove(name);
                }
            }
            if (now != was) {
                LOGGER.info(now ? "This node now runs the {} job" : "Another node now runs the {} job", name);
            }
        });
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

    /**
     * Runs the jobs that act on what time it is once, now, for tests that move the clock: the
     * engine's cluster-wide ones if this node holds the housekeeping lease, each extension leader
     * job whose lease it holds, then the extensions' every-node jobs, such as recurring tasks. The
     * engine's own every-node jobs (the heartbeat, the orphan sweep, topic reconciliation) don't
     * depend on the clock and keep their own schedule.
     */
    void runJobsNow() throws Exception {
        if (leading()) {
            for (HousekeepingJob job : coreJobs()) {
                if (job.scope() == HousekeepingJob.Scope.LEADER) {
                    job.run();
                }
            }
        }
        for (ExtensionJob each : extensionJobs) {
            if (each.leader() && leads(each.name())) {
                each.job().run();
            }
        }
        for (ExtensionJob each : extensionJobs) {
            if (!each.leader()) {
                each.job().run();
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
