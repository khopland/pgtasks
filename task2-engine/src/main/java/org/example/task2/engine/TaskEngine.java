package org.example.task2.engine;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.random.RandomGenerator;
import java.util.stream.Stream;

import org.example.task2.PayloadCodec;
import org.example.task2.Tasks;
import org.example.task2.TopicRef;
import org.example.task2.spi.Notifier;
import org.example.task2.spi.TaskStore;

/**
 * Processes topics on this node. On start it lets the store check or migrate its schema,
 * registers the node, creates every topic registered by name, and starts one poll loop per topic
 * it runs, plus the {@link Housekeeper}.
 *
 * <p>Which topics it runs follows the store: every topic registered by name or matching one of its
 * {@link TopicTemplate}s, unless paused. A topic created, paused or resumed on any node is
 * broadcast, and every node also compares the store's topics with what it runs on each heartbeat,
 * so a lost notification delays a change by at most one heartbeat. A paused topic's running
 * batches finish; nothing is claimed from it until it is resumed.
 *
 * <p>Wiring is plain constructor arguments for now; the configuration model with defaults,
 * per-module configurers and overrides comes in a later phase.
 */
public final class TaskEngine implements AutoCloseable {

    private static final System.Logger LOGGER = System.getLogger(TaskEngine.class.getName());

    private final TaskStore store;
    private final EngineSettings settings;
    private final List<Topic<?>> topics;
    private final Map<String, Topic<?>> topicsByName = new LinkedHashMap<>();
    private final List<TopicTemplate<?>> templates;
    private final TopicDirectory directory;
    private final DefaultTasks tasks;
    private final Map<String, TopicWorker<?>> workersByName = new ConcurrentHashMap<>();
    private final Map<Integer, TopicWorker<?>> workersById = new ConcurrentHashMap<>();
    private final Map<Long, Integer> held = new ConcurrentHashMap<>();
    /** Guards starting and stopping workers. */
    private final Object topicsLock = new Object();
    /** Workers of topics paused or gone, finishing their running batches, and the threads waiting for them. */
    private final Map<TopicWorker<?>, Thread> draining = new ConcurrentHashMap<>();
    private final Coalesced reconcileSoon = new Coalesced("topics", "topics", this::reconcileTopics);
    private EngineCore core;
    private Housekeeper housekeeper;
    private RateLimits limits;
    private Notifier notifier;
    private volatile int nodeId = -1;
    private List<EffectiveSettings> effectiveSettings = List.of();
    private volatile boolean started;

    public TaskEngine(TaskStore store, EngineSettings settings, List<Topic<?>> topics) {
        this(store, settings, topics, List.of());
    }

    public TaskEngine(TaskStore store, EngineSettings settings, List<Topic<?>> topics, List<TopicTemplate<?>> templates) {
        this.store = Objects.requireNonNull(store, "store");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.topics = List.copyOf(topics);
        this.templates = List.copyOf(templates);
        Map<String, PayloadCodec<?>> codecs = new LinkedHashMap<>();
        for (Topic<?> topic : this.topics) {
            if (codecs.put(topic.ref().name(), topic.codec()) != null) {
                throw new IllegalArgumentException("Topic " + topic.ref() + " is registered twice");
            }
            topicsByName.put(topic.ref().name(), topic);
        }
        this.directory = new TopicDirectory(store);
        this.tasks = new DefaultTasks(store, directory, codecs, this.templates, settings.codecs(), settings.transactions(), this::wake,
            settings.observer());
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
        List<RateLimit> configured = Stream.concat(topics.stream().map(topic -> topic.settings().rateLimit()),
            templates.stream().map(template -> template.settings().rateLimit())).filter(Objects::nonNull).toList();
        limits = new RateLimits(store, configured, this::limitChanged);
        core = new EngineCore(store, tasks, nodeId, new Semaphore(settings.maxInFlightBatches()), RandomGenerator.getDefault(), this::wake,
            held, () -> !started, limits, settings.observer());
        for (Topic<?> topic : topics) {
            int id = EngineCore.transact(store, transaction -> {
                int created = store.createTopic(transaction, topic.ref().name());
                store.setRetention(transaction, created, topic.settings().retention());
                return created;
            });
            directory.register(topic.ref().name(), id);
        }
        EngineCore.transact(store, transaction -> {
            for (RateLimit limit : configured) {
                store.ensureAdmission(transaction, limit.name(), limit.burst());
            }
            return null;
        });
        limits.refresh();
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
                public void topicChanged(String name) {
                    reconcileSoon.request();
                }

                @Override
                public void limitChanged(String name) {
                    limits.refreshSoon();
                }

                @Override
                public void missed() {
                    workersById.values().forEach(TopicWorker::wake);
                    reconcileSoon.request();
                    limits.refreshSoon();
                }
            });
        }
        housekeeper = new Housekeeper(store, settings, nodeId, this::wakeById, () -> releaseOrphans(orphanGrace()), () -> {
            reconcileTopics();
            limits.refresh();
        });
        housekeeper.start();
        started = true;
        try {
            reconcileTopics();
        } catch (Exception | Error e) {
            stop(Duration.ZERO);
            throw e;
        }
        return this;
    }

    /**
     * Brings the workers in line with the store: starts a worker for every topic this node runs
     * that isn't paused, and stops the workers of topics paused or gone, letting their running
     * batches finish. On start, on every heartbeat, and when a topic changes anywhere.
     */
    void reconcileTopics() throws Exception {
        if (!started) {
            return;
        }
        List<TaskStore.TopicInfo> registered = EngineCore.transact(store, store::topics);
        synchronized (topicsLock) {
            if (!started) {
                return;
            }
            Map<String, TaskStore.TopicInfo> wanted = new HashMap<>();
            for (TaskStore.TopicInfo info : registered) {
                if (!info.paused() && topicFor(info.name()) != null) {
                    wanted.put(info.name(), info);
                }
            }
            for (TopicWorker<?> worker : List.copyOf(workersByName.values())) {
                TaskStore.TopicInfo info = wanted.get(worker.topic().ref().name());
                if (info == null || info.id() != worker.topicId()) {
                    retire(worker);
                }
            }
            for (TaskStore.TopicInfo info : wanted.values()) {
                if (!workersByName.containsKey(info.name())) {
                    launch(info);
                }
            }
        }
    }

    /** The topic registered by name, or else made from the first template that matches; null if neither. */
    private Topic<?> topicFor(String name) {
        Topic<?> topic = topicsByName.get(name);
        if (topic != null) {
            return topic;
        }
        return templates.stream().filter(template -> template.matches(name)).findFirst().map(template -> template.instantiate(name)).orElse(null);
    }

    private void launch(TaskStore.TopicInfo info) throws Exception {
        Topic<?> topic = topicFor(info.name());
        if (!topicsByName.containsKey(info.name())) {
            // Topics registered by name record their retention at start; these show up later.
            EngineCore.transact(store, transaction -> {
                store.setRetention(transaction, info.id(), topic.settings().retention());
                return null;
            });
            LOGGER.log(System.Logger.Level.INFO, "Processing topic " + info.name() + " on this node");
        }
        directory.register(info.name(), info.id());
        TopicWorker<?> worker = new TopicWorker<>(topic, info.id(), core);
        workersByName.put(info.name(), worker);
        workersById.put(info.id(), worker);
        worker.start();
    }

    /** Stops polling a topic; its running batches finish within the topic's batch deadline. */
    private void retire(TopicWorker<?> worker) {
        String name = worker.topic().ref().name();
        workersByName.remove(name, worker);
        workersById.remove(worker.topicId(), worker);
        worker.stopPolling();
        LOGGER.log(System.Logger.Level.INFO, "Stopped processing topic " + name + " on this node; running batches finish");
        Thread drain = Thread.ofVirtual().name("task2-drain-" + name).unstarted(() -> {
            try {
                worker.awaitBatches(System.nanoTime() + worker.topic().settings().batchDeadline().toNanos());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                draining.remove(worker);
            }
        });
        draining.put(worker, drain);
        drain.start();
    }

    /**
     * Registers a topic, so every node with a handler for it starts processing it: one registered
     * by name, or matching a {@link TopicTemplate}. Idempotent.
     */
    public void createTopic(String name) throws Exception {
        TopicRef.of(name, Object.class);
        EngineCore.transact(store, transaction -> store.createTopic(transaction, name));
        reconcileTopics();
    }

    /** Pauses a topic on every node: pushes are still taken, nothing is claimed. Running batches finish. */
    public void pause(String topic) throws Exception {
        setPaused(topic, true);
    }

    /** Resumes a paused topic on every node. */
    public void resume(String topic) throws Exception {
        setPaused(topic, false);
    }

    private void setPaused(String topic, boolean paused) throws Exception {
        if (!EngineCore.transact(store, transaction -> store.setPaused(transaction, topic, paused))) {
            throw new IllegalArgumentException("Unknown topic '" + topic + "'");
        }
        reconcileTopics();
    }

    /**
     * Every topic's queue counts as the housekeeping leader last recorded them, by topic name. They
     * are at most one snapshot interval old (a minute by default), so dashboards and health checks
     * can read them as often as they like without counting the queue.
     */
    public List<TaskStore.TopicSnapshot> snapshots() throws Exception {
        return EngineCore.transact(store, store::snapshots);
    }

    /** The topics this node is processing now. */
    public Set<String> processing() {
        return started ? Set.copyOf(workersByName.keySet()) : Set.of();
    }

    /** The topic patterns this engine processes; see {@link TopicTemplate}. */
    public List<TopicTemplate<?>> templates() {
        return templates;
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

    /** The rate limits of this node's topics, and operators' runtime overrides of them. */
    public RateLimits limits() {
        if (limits == null) {
            throw new IllegalStateException("Not started");
        }
        return limits;
    }

    /** Tells the workers using a limit that it changed, so a wait for permits ends early. */
    private void limitChanged(String name) {
        workersById.values().stream()
            .filter(worker -> worker.topic().settings().rateLimit() != null && worker.topic().settings().rateLimit().name().equals(name))
            .forEach(TopicWorker::limitChanged);
    }

    /** Whether this node holds the housekeeping lease and runs the cluster-wide jobs. */
    public boolean leadsHousekeeping() {
        return started && housekeeper.leading();
    }

    /** Covers claims on their way in: committed, but not yet known to their worker. */
    private Duration orphanGrace() {
        Duration twoHeartbeats = settings.heartbeat().multipliedBy(2);
        return twoHeartbeats.compareTo(Duration.ofSeconds(2)) < 0 ? Duration.ofSeconds(2) : twoHeartbeats;
    }

    /**
     * Releases tasks this node holds active but isn't processing, such as a claim whose commit
     * succeeded while its reply was lost, if claimed longer ago than the grace period.
     */
    private void releaseOrphans(Duration grace) throws Exception {
        java.time.Instant cutoff = store.clock().instant().minus(grace);
        Map<Integer, List<TaskStore.Completion>> orphans = new HashMap<>();
        for (TaskStore.ActiveClaim claim : EngineCore.transact(store, transaction -> store.activeClaims(transaction, nodeId))) {
            if (!held.containsKey(claim.taskId()) && claim.claimedAt() != null && !claim.claimedAt().isAfter(cutoff)) {
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

    /**
     * Drains this node: stops claiming, lets running batches finish until the timeout, then
     * cancels what is left, whose tasks go back to the queue without counting the attempt. The
     * node keeps heartbeating while it drains, so no other node takes its tasks meanwhile.
     * Then it hands back any claim it holds but isn't processing, gives up the housekeeping lease,
     * and returns once every thread it started has ended. Threads that ignore cancellation get
     * {@link TopicWorker#PATIENCE} more, and are logged if they still run.
     */
    public synchronized void stop(Duration timeout) {
        if (!started) {
            return;
        }
        started = false;
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            // Nothing new starts: no wake-ups, no reloads.
            if (notifier != null) {
                notifier.close();
            }
            reconcileSoon.stop(TopicWorker.PATIENCE);
            limits.stopRefreshing(TopicWorker.PATIENCE);
            List<TopicWorker<?>> all = new java.util.ArrayList<>();
            List<Thread> drains;
            synchronized (topicsLock) {
                all.addAll(workersByName.values());
                all.addAll(draining.keySet());
                drains = List.copyOf(draining.values());
            }
            all.forEach(TopicWorker::stopPolling);
            for (TopicWorker<?> worker : all) {
                worker.awaitBatches(deadline);
            }
            for (Thread drain : drains) {
                drain.join(TopicWorker.PATIENCE);
            }
            workersByName.clear();
            workersById.clear();
            try {
                releaseOrphans(Duration.ZERO);
            } catch (Exception e) {
                LOGGER.log(System.Logger.Level.WARNING, "Could not hand back the tasks this node still holds; they are requeued once it counts as dead", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            housekeeper.stop();
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
