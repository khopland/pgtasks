package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
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
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.Schedule;
import io.github.khopland.pgtasks.spi.StoreTransaction;
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

    @FunctionalInterface
    interface Job {
        void run() throws Exception;
    }

    private final TaskStore store;
    private final EngineSettings settings;
    private final IntSupplier nodeId;
    private final Job rejoin;
    private final IntConsumer wake;
    private final Job sweepOrphans;
    private final Job reconcile;
    private final Object leadership = new Object();
    private final Object beating = new Object();
    private @Nullable Instant leaseEnds;
    private volatile @Nullable Instant lastHeartbeat;
    /**
     * Three threads, so a slow job can't hold up the others: the heartbeat and lease alone on
     * one (a node that misses them loses the lease, and its health turns DOWN), the regular jobs
     * on another, and retention, which can run long after downtime, on a third.
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
     *                     lost: the topics it runs and paused
     */
    Housekeeper(TaskStore store, EngineSettings settings, IntSupplier nodeId, Job rejoin, IntConsumer wake, Job sweepOrphans, Job reconcile,
                Duration purgeBudget) {
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
        schedule(beats, "heartbeat", settings.heartbeat(), this::heartbeat);
        schedule(jobs, "orphan sweep", settings.heartbeat(), sweepOrphans);
        schedule(jobs, "reconcile", settings.heartbeat(), reconcile);
        schedule(jobs, "expiry", settings.heartbeat(), () -> {
            if (leading()) {
                expire();
            }
        });
        schedule(jobs, "promotion", settings.promoteInterval(), () -> {
            if (leading()) {
                promote();
            }
        });
        schedule(jobs, "recurring tasks", settings.promoteInterval(), () -> {
            if (leading()) {
                fireSchedules();
            }
        });
        schedule(slow, "retention", settings.purgeInterval(), () -> {
            if (leading()) {
                purge();
            }
        });
        schedule(jobs, "count snapshot", settings.snapshotInterval(), () -> {
            if (leading()) {
                snapshot();
            }
        });
    }

    private ScheduledExecutorService executor(String name) {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, Thread.ofVirtual().name(name, 0).factory());
        executors.add(executor);
        return executor;
    }

    @SuppressWarnings("FutureReturnValueIgnored") // the job logs its own failures; stop() shuts the executor down
    private void schedule(ScheduledExecutorService executor, String name, Duration interval, Job job) {
        executor.scheduleWithFixedDelay(() -> {
            try {
                job.run();
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
        Instant beat = store.clock().instant();
        int node = nodeId.getAsInt();
        if (!EngineCore.transact(store, transaction -> store.heartbeat(transaction, node))) {
            synchronized (leadership) {
                leaseEnds = null;
            }
            rejoin.run();
        }
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

    /** If this node leads, runs the cluster-wide jobs once, now: for tests that move the clock. */
    void runLeaderJobsNow() throws Exception {
        if (leading()) {
            expire();
            promote();
            fireSchedules();
            purge();
            snapshot();
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

    /** How soon a recurring task whose push failed is tried again, unless its next run comes first. */
    static final Duration FAILED_SCHEDULE_RETRY = Duration.ofMinutes(1);

    /**
     * Pushes the recurring tasks that are due, each in the same transaction as moving its next run
     * on, so a schedule fires once even if this node dies halfway. A run missed while no node was
     * up is made up once: the next run counts from now. Each schedule has a savepoint of its own,
     * so one whose push fails is undone, logged and tried again later, and the others still fire.
     */
    private void fireSchedules() throws Exception {
        int[] due = new int[1];
        do {
            // Seen by the observer like any push: traced while written, counted once committed.
            List<Runnable> committed = new ArrayList<>();
            List<Integer> fired = EngineCore.transact(store, transaction -> {
                committed.clear();
                Instant now = store.clock().instant();
                List<Integer> topics = new ArrayList<>();
                List<Schedule> schedules = store.dueSchedules(transaction, BATCH);
                due[0] = schedules.size();
                for (Schedule schedule : schedules) {
                    Instant next;
                    try {
                        // The store decides what is due by its own clock (the database server's), this
                        // node by the JVM's. From whichever is later, so a node a few milliseconds behind
                        // the server can't give back the slot it just fired and fire it again.
                        Instant from = now.isAfter(schedule.nextRunAt()) ? now : schedule.nextRunAt();
                        next = Cron.parse(schedule.cron(), ZoneId.of(schedule.zone())).following(from);
                    } catch (RuntimeException e) {
                        // Written by a node with another idea of cron; try again in an hour rather than every second.
                        LOGGER.warn("Recurring task {} of {} has a schedule this node can't read, '{}' in {}; skipped for an hour",
                            schedule.name(), schedule.topic(), schedule.cron(), schedule.zone(), e);
                        store.scheduleFired(transaction, schedule.id(), now, now.plus(Duration.ofHours(1)));
                        continue;
                    }
                    TopicRef<?> topic = TopicRef.of(schedule.topic(), Object.class);
                    StoreTransaction.Savepoint savepoint = transaction.savepoint();
                    List<Pushed> results;
                    try {
                        try (TaskObserver.PushObservation observation = settings.observer().pushing(topic, 1)) {
                            results = store.push(transaction, schedule.topicId(), schedule.insertion(), List.of(new TaskStore.NewTask(schedule.identifier(),
                                schedule.payload(), schedule.payloadType(), null, null, false, 0, null, null, null, observation.headers())));
                        }
                        store.scheduleFired(transaction, schedule.id(), now, next);
                        savepoint.release();
                    } catch (Exception e) {
                        // Undone, or the transaction can't go on (a lost connection): then this throws and the whole run is retried.
                        savepoint.rollback();
                        if (Transient.is(e)) {
                            throw e;
                        }
                        // Its slot stays unfired: tried again shortly, and made up once when it works.
                        Instant retry = now.plus(FAILED_SCHEDULE_RETRY);
                        Instant at = next.isBefore(retry) ? next : retry;
                        LOGGER.warn("Recurring task {} of {} could not be pushed; tried again at {}", schedule.name(), schedule.topic(), at, e);
                        store.scheduleFired(transaction, schedule.id(), now, at);
                        continue;
                    }
                    committed.add(() -> settings.observer().pushed(topic, results));
                    topics.add(schedule.topicId());
                }
                return topics;
            });
            committed.forEach(Runnable::run);
            fired.forEach(wake::accept);
        } while (due[0] == BATCH && leading());
    }

    /**
     * Deletes finished tasks past their retention, counted from completion, a batch at a time,
     * for at most {@link #PURGE_BUDGET}: after downtime, or after retention was shortened, the rest
     * waits for the next run rather than holding the job. A run that runs out of time leaves the
     * next run to start with the following topic, so a topic with a large backlog can't keep the
     * others from ever being purged.
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
                Instant cutoff = now.atZone(ZoneOffset.UTC).minus(kept.getValue()).toInstant();
                int deleted;
                do {
                    deleted = EngineCore.transact(store, transaction -> store.purge(transaction, topic, kept.getKey(), cutoff, BATCH));
                    if (System.nanoTime() > deadline) {
                        purgeStoppedAt = topic;
                        LOGGER.info("Retention used its {} for this run; it goes on in {}", purgeBudget, settings.purgeInterval());
                        return;
                    }
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
        return "Housekeeper[node " + nodeId.getAsInt() + (leading() ? ", leading" : "") + "]";
    }
}
