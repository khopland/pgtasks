package org.example.task2.engine;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.random.RandomGenerator;

import org.example.task2.PayloadCodec;
import org.example.task2.Tasks;
import org.example.task2.spi.Notifier;
import org.example.task2.spi.TaskStore;

/**
 * Processes topics on this node. On start it lets the store check or migrate its schema,
 * registers the node, creates every registered topic, and starts one poll loop per topic plus
 * the {@link Housekeeper}.
 *
 * <p>Wiring is plain constructor arguments for now; the configuration model with defaults,
 * per-module configurers and overrides comes in a later phase.
 */
public final class TaskEngine implements AutoCloseable {

    private static final System.Logger LOGGER = System.getLogger(TaskEngine.class.getName());

    private final TaskStore store;
    private final EngineSettings settings;
    private final List<Topic<?>> topics;
    private final TopicDirectory directory;
    private final DefaultTasks tasks;
    private final Map<String, TopicWorker<?>> workersByName = new ConcurrentHashMap<>();
    private final Map<Integer, TopicWorker<?>> workersById = new ConcurrentHashMap<>();
    private final Map<Long, Integer> held = new ConcurrentHashMap<>();
    private Housekeeper housekeeper;
    private Notifier notifier;
    private volatile int nodeId = -1;
    private List<EffectiveSettings> effectiveSettings = List.of();
    private volatile boolean started;

    public TaskEngine(TaskStore store, EngineSettings settings, List<Topic<?>> topics) {
        this.store = Objects.requireNonNull(store, "store");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.topics = List.copyOf(topics);
        Map<String, PayloadCodec<?>> codecs = new LinkedHashMap<>();
        for (Topic<?> topic : this.topics) {
            if (codecs.put(topic.ref().name(), topic.codec()) != null) {
                throw new IllegalArgumentException("Topic " + topic.ref() + " is registered twice");
            }
        }
        this.directory = new TopicDirectory(store);
        this.tasks = new DefaultTasks(store, directory, codecs, settings.codecs(), settings.transactions(), this::wake);
    }

    /** Builds an engine from configurers, with defaults for everything they don't set. */
    public static TaskEngineBuilder builder() {
        return new TaskEngineBuilder();
    }

    TaskEngine withEffectiveSettings(List<EffectiveSettings> effectiveSettings) {
        this.effectiveSettings = List.copyOf(effectiveSettings);
        return this;
    }

    /** Each topic's settings and where each value came from; filled in when built with {@link #builder()}. */
    public List<EffectiveSettings> effectiveSettings() {
        return effectiveSettings;
    }

    /** Transaction-aware enqueue. Usable before {@link #start()}, for topics that already exist. */
    public Tasks tasks() {
        return tasks;
    }

    public int nodeId() {
        if (nodeId < 0) {
            throw new IllegalStateException("Not started");
        }
        return nodeId;
    }

    public synchronized TaskEngine start() throws Exception {
        if (started) {
            throw new IllegalStateException("Already started");
        }
        store.prepare();
        nodeId = EngineCore.transact(store, transaction -> store.registerNode(transaction, settings.nodeName(), version()));
        EngineCore core = new EngineCore(store, tasks, nodeId, new Semaphore(settings.maxInFlightBatches()), RandomGenerator.getDefault(), this::wake,
            held, () -> !started);
        Map<Topic<?>, Integer> ids = new HashMap<>();
        for (Topic<?> topic : topics) {
            int id = EngineCore.transact(store, transaction -> {
                int created = store.createTopic(transaction, topic.ref().name());
                store.setRetention(transaction, created, topic.settings().retention());
                if (topic.settings().rateLimit() != null) {
                    store.ensureAdmission(transaction, topic.settings().rateLimit().name(), topic.settings().rateLimit().burst());
                }
                return created;
            });
            directory.register(topic.ref().name(), id);
            ids.put(topic, id);
        }
        for (Topic<?> topic : topics) {
            TopicWorker<?> worker = new TopicWorker<>(topic, ids.get(topic), core);
            workersByName.put(topic.ref().name(), worker);
            workersById.put(ids.get(topic), worker);
        }
        notifier = settings.notifier();
        if (notifier != null) {
            notifier.start(new Notifier.Listener() {
                @Override
                public void ready(int topicId) {
                    TopicWorker<?> worker = workersById.get(topicId);
                    if (worker != null) {
                        worker.wake();
                    }
                }

                @Override
                public void missed() {
                    workersById.values().forEach(TopicWorker::wake);
                }
            });
        }
        housekeeper = new Housekeeper(store, settings, nodeId, this::wakeById, this::releaseOrphans);
        housekeeper.start();
        workersByName.values().forEach(TopicWorker::start);
        started = true;
        return this;
    }

    TaskStore store() {
        return store;
    }

    /** The topics this engine processes. */
    public List<Topic<?>> topics() {
        return topics;
    }

    DefaultTasks defaultTasks() {
        return tasks;
    }

    /** The running workers; none once the engine has stopped. */
    java.util.Collection<TopicWorker<?>> workers() {
        return started ? workersByName.values() : List.of();
    }

    /** Heartbeats and renews or takes the housekeeping lease, now. */
    void heartbeatNow() throws Exception {
        if (started) {
            housekeeper.heartbeat();
        }
    }

    /** If this node leads, runs expiry, promotion, retention and count snapshots once, now. */
    void runLeaderJobsNow() throws Exception {
        if (started) {
            housekeeper.runLeaderJobsNow();
        }
    }

    /** Whether this node holds the housekeeping lease and runs the cluster-wide jobs. */
    public boolean leadsHousekeeping() {
        return started && housekeeper.leading();
    }

    /**
     * Releases tasks this node holds active but isn't processing, such as a claim whose commit
     * succeeded while its reply was lost. A grace period covers claims that are on their way in.
     */
    private void releaseOrphans() throws Exception {
        Duration grace = settings.heartbeat().multipliedBy(2).compareTo(Duration.ofSeconds(2)) < 0 ? Duration.ofSeconds(2) : settings.heartbeat().multipliedBy(2);
        java.time.Instant cutoff = store.clock().instant().minus(grace);
        Map<Integer, List<TaskStore.Completion>> orphans = new HashMap<>();
        for (TaskStore.ActiveClaim claim : EngineCore.transact(store, transaction -> store.activeClaims(transaction, nodeId))) {
            if (!held.containsKey(claim.taskId()) && claim.claimedAt() != null && claim.claimedAt().isBefore(cutoff)) {
                orphans.computeIfAbsent(claim.topicId(), ignored -> new java.util.ArrayList<>())
                    .add(new TaskStore.Completion(claim.taskId(), claim.attempt(), claim.claimedAt(), BatchRun.RELEASED));
            }
        }
        for (Map.Entry<Integer, List<TaskStore.Completion>> entry : orphans.entrySet()) {
            int released = EngineCore.transact(store, transaction -> store.release(transaction, entry.getKey(), nodeId, entry.getValue()));
            if (released > 0) {
                LOGGER.log(System.Logger.Level.WARNING, "Released " + released + " task(s) this node held but wasn't processing");
                wakeById(entry.getKey());
            }
        }
    }

    /** Wakes this node's worker for a topic, if it has one. */
    public void wake(String topic) {
        TopicWorker<?> worker = workersByName.get(topic);
        if (worker != null) {
            worker.wake();
        }
    }

    private void wakeById(int topicId) {
        TopicWorker<?> worker = workersById.get(topicId);
        if (worker != null) {
            worker.wake();
        }
    }

    /** Stops polling, waits up to the timeout for running batches, then cancels what is left. */
    public synchronized void stop(Duration timeout) {
        if (!started) {
            return;
        }
        started = false;
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            workersByName.values().forEach(TopicWorker::stopPolling);
            for (TopicWorker<?> worker : workersByName.values()) {
                worker.awaitBatches(deadline);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            housekeeper.stop();
            if (notifier != null) {
                notifier.close();
            }
        }
    }

    @Override
    public void close() {
        stop(Duration.ofSeconds(30));
    }

    private static String version() {
        String version = TaskEngine.class.getPackage().getImplementationVersion();
        return version == null ? "dev" : version;
    }

    @Override
    public String toString() {
        return "TaskEngine[" + settings.nodeName() + ", " + store + "]";
    }
}
