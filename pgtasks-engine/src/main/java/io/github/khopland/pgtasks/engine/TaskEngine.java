package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.random.RandomGenerator;
import java.util.stream.Collectors;

import io.github.khopland.pgtasks.BatchHandler;
import io.github.khopland.pgtasks.PayloadCodec;
import io.github.khopland.pgtasks.TaskStorageException;
import io.github.khopland.pgtasks.TaskWriter;
import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.extension.BatchListener;
import io.github.khopland.pgtasks.engine.extension.ClaimGate;
import io.github.khopland.pgtasks.engine.extension.Extension;
import io.github.khopland.pgtasks.engine.extension.ExtensionContext;
import io.github.khopland.pgtasks.engine.extension.HandlerDecorator;
import io.github.khopland.pgtasks.engine.extension.HousekeepingJob;
import io.github.khopland.pgtasks.engine.extension.OperatorView;
import io.github.khopland.pgtasks.engine.extension.SettingKey;
import io.github.khopland.pgtasks.spi.Notifier;
import io.github.khopland.pgtasks.spi.StoreCapability;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes topics on this node. On start it lets the store check or migrate its schema,
 * registers the node, creates every topic registered by name, and starts one poll loop per topic
 * it runs, plus housekeeping.
 *
 * <p>Which topics it runs follows the store: every topic registered by name that the store has. A
 * topic deleted on any node is broadcast, and every node also compares the store's topics with
 * what it runs on each heartbeat, so a lost notification delays a change by at most one
 * heartbeat. A deleted topic's running batches finish, and nothing more is claimed from it.
 *
 * <p>Built with {@link #builder()}. Producers use {@link #tasks()}, operators {@link #admin()}.
 * Every method throws only unchecked exceptions.
 */
public final class TaskEngine implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskEngine.class);

    private final TaskStore store;
    private final EngineSettings settings;
    private final List<Topic<?>> topics;
    private final Map<String, Topic<?>> topicsByName = new LinkedHashMap<>();
    private final TopicDirectory directory;
    private final DefaultTasks tasks;
    private final TaskAdmin admin;
    private final Map<String, TopicWorker<?>> workersByName = new ConcurrentHashMap<>();
    private final Map<Integer, TopicWorker<?>> workersById = new ConcurrentHashMap<>();
    private final Map<Long, Integer> held = new ConcurrentHashMap<>();
    /** Guards starting and stopping workers. */
    private final Object topicsLock = new Object();
    /** Serializes reconciles, each from reading the topics to its last change. */
    private final Object reconcileLock = new Object();
    /** Workers of topics gone, finishing their running batches, and the threads waiting for them. */
    private final Map<TopicWorker<?>, Thread> draining = new ConcurrentHashMap<>();
    private final Coalesced reconcileSoon = new Coalesced("topics", "topics", this::reconcileTopics);
    private @Nullable EngineCore core;
    private @Nullable Housekeeper housekeeper;
    private @Nullable Notifier notifier;
    private volatile int nodeId = -1;
    private List<EffectiveSettings> effectiveSettings = List.of();
    private @Nullable String version;
    private volatile boolean started;
    private volatile boolean prepared;
    /** Whether it was started and stopped before, so a start must switch the reloads back on. */
    private boolean stopped;
    /** What installed extensions registered; see {@link #install}. */
    private final List<HousekeepingJob> extensionJobs = new ArrayList<>();
    private final List<ClaimGate> gates = new ArrayList<>();
    private final List<BatchListener> batchListeners = new ArrayList<>();
    private final List<HandlerDecorator> decorators = new ArrayList<>();
    private final List<Runnable> onStart = new ArrayList<>();
    private final List<Runnable> onStop = new ArrayList<>();
    private final List<Extension> extensions = new ArrayList<>();
    private final Map<String, OperatorView> views = new LinkedHashMap<>();
    /** The builder's observers, and those installed extensions add. */
    private TaskObserver observer;
    /** Each extension channel's reloads, run soon on a message, a reconnect or a heartbeat. */
    private final Map<String, List<ChannelReload>> channels = new LinkedHashMap<>();
    /** How long one retention run may take; shorter in tests. */
    Duration purgeBudget = Housekeeper.PURGE_BUDGET;

    TaskEngine(TaskStore store, EngineSettings settings, List<Topic<?>> topics) {
        this.store = Objects.requireNonNull(store, "store");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.topics = List.copyOf(topics);
        Map<String, PayloadCodec<?>> codecs = new LinkedHashMap<>();
        for (Topic<?> topic : this.topics) {
            if (codecs.put(topic.ref().name(), topic.codec()) != null) {
                throw new IllegalArgumentException("Topic " + topic.ref() + " is registered twice");
            }
            topicsByName.put(topic.ref().name(), topic);
        }
        this.directory = new TopicDirectory(store);
        this.admin = new TaskAdmin(this, store);
        this.observer = settings.observer();
        this.tasks = new DefaultTasks(store, directory, codecs, settings.codecs(), settings.transactions(), this::wakeById, observer);
    }

    /** Builds an engine from configurers, with defaults for everything they don't set. */
    public static TaskEngineBuilder builder() {
        return new TaskEngineBuilder();
    }

    TaskEngine withEffectiveSettings(List<EffectiveSettings> effectiveSettings) {
        this.effectiveSettings = List.copyOf(effectiveSettings);
        return this;
    }

    /** Topics this node's modules push to, created when it prepares; see {@link TaskRegistry#pushesTo}. */
    private Set<String> pushedTo = Set.of();

    TaskEngine withPushedTo(Collection<String> topics) {
        this.pushedTo = Set.copyOf(topics);
        return this;
    }

    TaskEngine withVersion(@Nullable String version) {
        this.version = version;
        return this;
    }

    TaskEngine withFallbackCodecs(@Nullable PayloadCodecs codecs) {
        tasks.fallbackCodecs(codecs);
        return this;
    }

    TaskEngine withTopicsCreatedOnPush() {
        directory.createOnPush();
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

    /** The operator's side: topics, tasks, dead letters and retention, across the cluster. */
    public TaskAdmin admin() {
        return admin;
    }

    /** Whether this node's configuration registers the topic by name. */
    boolean registersByName(String topic) {
        return topicsByName.containsKey(topic);
    }

    public int nodeId() {
        if (nodeId < 0) {
            throw new IllegalStateException("Not started");
        }
        return nodeId;
    }

    /**
     * Readies the store for this engine without processing anything: applies the schema (as the
     * store's schema mode says), registers the configured topics with their retention and
     * and the topics its modules push to. Enough for a node that only pushes, such as the
     * Spring starter's {@code pgtasks.enabled=false}: its pushes work whichever node starts first,
     * and the nodes that process the topics pick them up. {@link #start()} does this first.
     */
    public synchronized TaskEngine prepare() {
        Unchecked.run("Preparing the task store", this::prepareStore);
        return this;
    }

    private void prepareStore() throws Exception {
        if (prepared) {
            return;
        }
        store.prepare();
        for (Topic<?> topic : topics) {
            int id = EngineCore.transact(store, transaction -> {
                int created = store.createTopic(transaction, topic.ref().name());
                store.setRetention(transaction, created, topic.settings().retention());
                return created;
            });
            directory.register(topic.ref().name(), id);
        }
        for (String topic : pushedTo) {
            if (!topicsByName.containsKey(topic)) {
                directory.register(topic, EngineCore.transact(store, transaction -> store.createTopic(transaction, topic)));
            }
        }
        prepared = true;
    }

    /** Whether the store has been readied for this engine: {@link #prepare()} or {@link #start()} has run. */
    public boolean prepared() {
        return prepared;
    }

    /** Whether this engine processes tasks: started and not stopped. A node that only pushes never starts. */
    public boolean running() {
        return started;
    }

    /**
     * Prepares the store, registers this node and starts processing: a poll loop per topic it
     * runs, housekeeping, and listening for notifications. Can be called again after
     * {@link #stop}.
     *
     * @throws TaskStorageException if the store can't be reached or refuses
     */
    public synchronized TaskEngine start() {
        if (started) {
            throw new IllegalStateException("Already started");
        }
        Unchecked.run("Starting the task engine", this::startProcessing);
        return this;
    }

    private void startProcessing() throws Exception {
        prepare();
        if (stopped) {
            // Started again after a stop, as Spring does on context.stop() and start(): the
            // reloads that stop() switched off take notifications again. It registers as a new node.
            reconcileSoon.restart();
            channels.values().forEach(reloads -> reloads.forEach(ChannelReload::restart));
        }
        // Started before registering, which records the first heartbeat, so the fence errs early.
        HeartbeatFence fence = new HeartbeatFence(settings.heartbeat(), settings.expiry(), System::nanoTime);
        nodeId = EngineCore.transact(store, transaction -> store.registerNode(transaction, settings.nodeName(), version, settings.expiry()));
        core = new EngineCore(store, tasks, () -> nodeId, new Semaphore(settings.maxInFlightBatches()), RandomGenerator.getDefault(), this::wake,
            held, () -> !started, observer, List.copyOf(gates), List.copyOf(batchListeners), fence::fenced,
            Thread::ofVirtual);
        Notifier wakeUps = settings.notifier();
        notifier = wakeUps;
        housekeeper = null;
        try {
            if (wakeUps != null) {
                Set<String> listened = new LinkedHashSet<>(channels.keySet());
                listened.add(Notifier.TOPIC);
                wakeUps.start(new Notifier.Listener() {
                    @Override
                    public void ready(int topicId) {
                        TopicWorker<?> worker = workersById.get(topicId);
                        if (worker != null) {
                            worker.wake();
                        }
                    }

                    @Override
                    public void notified(String channel, String payload) {
                        if (channel.equals(Notifier.TOPIC)) {
                            reconcileSoon.request();
                        } else {
                            channels.getOrDefault(channel, List.of()).forEach(reload -> reload.message(payload));
                        }
                    }

                    @Override
                    public void missed() {
                        workersById.values().forEach(TopicWorker::wake);
                        reconcileSoon.request();
                        reloadChannels();
                    }
                }, listened);
            }
            Housekeeper keeper = new Housekeeper(store, settings, () -> nodeId, this::rejoin, this::wakeById, () -> releaseOrphans(orphanGrace()), () -> {
                reconcileTopics();
                reloadChannels();
            }, purgeBudget, extensionJobs, fence, new Housekeeper.Fencing() {
                @Override
                public void fenced() {
                    allWorkers().forEach(TopicWorker::fence);
                }

                @Override
                public void lifted() {
                    allWorkers().forEach(TopicWorker::wake);
                }
            });
            housekeeper = keeper;
            // Before the jobs and the topics: an extension readies what its gates and jobs use, and
            // one that can't fails the start before anything is claimed.
            startExtensions();
            keeper.start();
            started = true;
            reconcileTopics();
        } catch (Exception | Error e) {
            if (started) {
                stop(Duration.ZERO);
            } else {
                // Not started, so stop() would do nothing: end what this start got going.
                stopExtensions();
                if (wakeUps != null) {
                    wakeUps.close();
                }
                Housekeeper keeper = housekeeper;
                if (keeper != null) {
                    keeper.stop();
                }
                deregister();
            }
            throw e;
        }
    }

    /**
     * Brings the workers in line with the store: starts a worker for every topic this node runs,
     * and stops the workers of topics gone, letting their running
     * batches finish. On start, on every heartbeat, and when a topic changes anywhere.
     */
    void reconcileTopics() throws Exception {
        // One reconcile at a time, from its read to its last change, so two can't act on different reads.
        synchronized (reconcileLock) {
            reconcileTopicsNow();
        }
    }

    private void reconcileTopicsNow() throws Exception {
        if (!started) {
            return;
        }
        List<TaskStore.TopicInfo> registered = EngineCore.transact(store, store::topics);
        // A topic deleted anywhere is unknown here too: a push to it fails as unknown.
        directory.retain(registered.stream().map(TaskStore.TopicInfo::name).collect(Collectors.toSet()));
        synchronized (topicsLock) {
            if (!started) {
                return;
            }
            Map<String, TaskStore.TopicInfo> wanted = new HashMap<>();
            for (TaskStore.TopicInfo info : registered) {
                if (topicsByName.containsKey(info.name())) {
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

    private void launch(TaskStore.TopicInfo info) throws Exception {
        Topic<?> topic = decorated(Objects.requireNonNull(topicsByName.get(info.name()), info.name()));
        directory.register(info.name(), info.id());
        TopicWorker<?> worker = new TopicWorker<>(topic, info.id(), running(core));
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
        LOGGER.info("Stopped processing topic {} on this node; running batches finish", name);
        Thread drain = Thread.ofVirtual().name("pgtasks-drain-" + name).unstarted(() -> {
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
     * How this node is doing: heartbeat, leadership, notifications and claims stuck past their
     * deadline. Reads this node's active claims, so it fails if the database can't be reached.
     */
    public EngineHealth health() {
        return Unchecked.call("Checking the engine's health", this::checkHealth);
    }

    private EngineHealth checkHealth() throws Exception {
        Duration term = settings.leaseTerm();
        if (!started) {
            return new EngineHealth(false, settings.nodeName(), nodeId, null, null, term, false, EngineHealth.Notifications.NONE, List.of(),
                List.of(), List.of());
        }
        Instant now = store.clock().instant();
        Instant beat = running(housekeeper).lastHeartbeat();
        Notifier wakeUps = notifier;
        EngineHealth.Notifications notifications = wakeUps == null ? EngineHealth.Notifications.NONE
            : wakeUps.connected() ? EngineHealth.Notifications.CONNECTED : EngineHealth.Notifications.DISCONNECTED;
        List<EngineHealth.StuckClaim> stuck = new ArrayList<>();
        for (TaskStore.ActiveClaim claim : EngineCore.transact(store, transaction -> store.activeClaims(transaction, nodeId))) {
            TopicWorker<?> worker = workersById.get(claim.topicId());
            Duration allowed = (worker == null ? term : worker.topic().settings().batchDeadline()).plus(settings.heartbeat());
            Duration running = claim.claimedAt() == null ? Duration.ZERO : Duration.between(claim.claimedAt(), now);
            if (running.compareTo(allowed) > 0) {
                stuck.add(new EngineHealth.StuckClaim(worker == null ? directory.name(claim.topicId()) : worker.topic().ref().name(),
                    claim.taskId(), claim.attempt(), claim.claimedAt(), running));
            }
        }
        return new EngineHealth(true, settings.nodeName(), nodeId, beat, beat == null ? null : Duration.between(beat, now), term,
            running(housekeeper).leading(), notifications, stuck, heldTopics(), running(housekeeper).jobs());
    }

    /**
     * The topics an extension's claim gate holds back on this node now, such as a rate limit or a
     * pause, with since when; none once the engine has stopped. Read from memory, without the store.
     */
    public List<EngineHealth.HeldTopic> heldTopics() {
        return workers().stream().map(TopicWorker::heldBack).filter(Objects::nonNull).map(Objects::requireNonNull)
            .sorted(Comparator.comparing(EngineHealth.HeldTopic::topic)).toList();
    }

    /** What {@link #start} set up, while the engine runs. */
    private static <T> T running(@Nullable T part) {
        if (part == null) {
            throw new IllegalStateException("The engine is not running");
        }
        return part;
    }

    /** The topics this node is processing now. */
    public Set<String> processing() {
        return started ? Set.copyOf(workersByName.keySet()) : Set.of();
    }

    TaskStore store() {
        return store;
    }

    EngineSettings settings() {
        return settings;
    }

    /** The topics this engine registers by name. */
    List<Topic<?>> topics() {
        return topics;
    }

    DefaultTasks defaultTasks() {
        return tasks;
    }

    /** The running workers; none once the engine has stopped. */
    Collection<TopicWorker<?>> workers() {
        return started ? workersByName.values() : List.of();
    }

    /** Heartbeats and renews or takes the housekeeping lease, now. */
    void heartbeatNow() throws Exception {
        if (started) {
            running(housekeeper).heartbeat();
        }
    }

    /**
     * Runs the jobs that act on what time it is once, now, for tests that move the clock: those
     * this node leads, the engine's and its extensions', then the extensions' every-node jobs.
     */
    void runJobsNow() throws Exception {
        if (started) {
            running(housekeeper).runJobsNow();
        }
    }

    /** Whether this node holds the housekeeping lease and runs the cluster-wide jobs. */
    public boolean leadsHousekeeping() {
        return started && running(housekeeper).leading();
    }

    /**
     * Registers this node again, under a new id, after the cluster forgot it: its heartbeat found
     * no row. It holds no claims in the store (a node with claims is never forgotten), so whatever
     * its workers still run under the old id completes as stale and is discarded, and the workers
     * claim under the new id from their next claim on.
     */
    private void rejoin() throws Exception {
        int forgotten = nodeId;
        int fresh = EngineCore.transact(store, transaction -> store.registerNode(transaction, settings.nodeName(), version, settings.expiry()));
        nodeId = fresh;
        LOGGER.error("The cluster forgot node {} ({}): it was silent for too long, as after a suspended VM or a long network partition. "
            + "It registered again as node {}; whatever it was still running under the old id is discarded", forgotten, settings.nodeName(), fresh);
    }

    /** Covers claims on their way in: committed, but not yet known to their worker. */
    private Duration orphanGrace() {
        Duration twoHeartbeats = settings.heartbeat().multipliedBy(2);
        return twoHeartbeats.compareTo(Duration.ofSeconds(2)) < 0 ? Duration.ofSeconds(2) : twoHeartbeats;
    }

    /**
     * Releases tasks this node holds active but isn't processing, such as a claim whose commit
     * succeeded while its reply was lost, if claimed longer ago than the grace period. The store
     * applies the grace by the clock it stamped the claims with, so a node whose clock runs ahead
     * of the database's doesn't release claims still on their way to their worker.
     */
    private void releaseOrphans(Duration grace) throws Exception {
        int node = nodeId;
        Map<Integer, List<TaskStore.Completion>> orphans = new HashMap<>();
        for (TaskStore.ActiveClaim claim : EngineCore.transact(store, transaction -> store.activeClaims(transaction, node, grace))) {
            if (!held.containsKey(claim.taskId())) {
                orphans.computeIfAbsent(claim.topicId(), ignored -> new ArrayList<>())
                    .add(new TaskStore.Completion(claim.taskId(), claim.attempt(), claim.claimedAt(), BatchRun.RELEASED));
            }
        }
        for (Map.Entry<Integer, List<TaskStore.Completion>> entry : orphans.entrySet()) {
            int released = EngineCore.transact(store, transaction -> store.release(transaction, entry.getKey(), node, entry.getValue()));
            if (released > 0) {
                LOGGER.warn("Released {} task(s) this node held but wasn't processing", released);
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
     * deregisters the node, and returns once every thread it started has ended. Threads that ignore cancellation get
     * {@link TopicWorker#PATIENCE} more, and are logged if they still run.
     */
    public synchronized void stop(Duration timeout) {
        if (!started) {
            return;
        }
        started = false;
        stopped = true;
        long deadline = System.nanoTime() + timeout.toNanos();
        stopExtensions();
        try {
            // Nothing new starts: no wake-ups, no reloads.
            Notifier wakeUps = notifier;
            if (wakeUps != null) {
                wakeUps.close();
            }
            reconcileSoon.stop(TopicWorker.PATIENCE);
            for (List<ChannelReload> reloads : channels.values()) {
                for (ChannelReload reload : reloads) {
                    reload.stop(TopicWorker.PATIENCE);
                }
            }
            List<TopicWorker<?>> all = new ArrayList<>();
            List<Thread> drains;
            synchronized (topicsLock) {
                all.addAll(workersByName.values());
                all.addAll(draining.keySet());
                drains = List.copyOf(draining.values());
            }
            all.forEach(TopicWorker::stopPolling);
            // One deadline and one patience for every topic together: in turn, each topic whose
            // handlers ignore cancellation added its own, and a stop could run past the grace
            // period a platform gives it (30 s in Kubernetes) before handing the tasks back.
            for (TopicWorker<?> worker : all) {
                worker.awaitUntil(deadline);
            }
            all.forEach(TopicWorker::cancel);
            long patience = System.nanoTime() + TopicWorker.PATIENCE.toNanos();
            for (TopicWorker<?> worker : all) {
                worker.awaitCancelled(patience);
            }
            for (Thread drain : drains) {
                drain.join(Duration.ofNanos(Math.max(0, patience - System.nanoTime())));
            }
            try {
                releaseOrphans(Duration.ZERO);
            } catch (Exception e) {
                LOGGER.warn("Could not hand back the tasks this node still holds; they are requeued once it counts as dead", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // Also when interrupted: a worker left here would keep its topic from being launched on the next start.
            forgetWorkers();
            Housekeeper keeper = housekeeper;
            if (keeper != null) {
                keeper.stop();
            }
            deregister();
        }
    }

    /**
     * Removes this node's row once its heartbeat has stopped, so operators don't see a stopped
     * node as one that died. A node still holding claims (it couldn't hand them back) stays until
     * it counts as dead, and another node requeues them. A later start registers a new node.
     */
    private void deregister() {
        int id = nodeId;
        if (id < 0) {
            return;
        }
        try {
            if (!EngineCore.transact(store, transaction -> store.deregisterNode(transaction, id))) {
                LOGGER.info("Node {} ({}) still holds claims, so it stays registered until it counts as dead", id, settings.nodeName());
            }
        } catch (Exception e) {
            LOGGER.warn("Could not deregister node {} ({}); it is forgotten once it counts as dead", id, settings.nodeName(), e);
        }
    }

    /** The workers polling now, and those of retired topics whose batches still run. */
    private List<TopicWorker<?>> allWorkers() {
        synchronized (topicsLock) {
            List<TopicWorker<?>> all = new ArrayList<>(workersByName.values());
            all.addAll(draining.keySet());
            return all;
        }
    }

    /** Drops the stopped workers, so the next start launches its own. */
    private void forgetWorkers() {
        synchronized (topicsLock) {
            workersByName.clear();
            workersById.clear();
        }
    }

    @Override
    public void close() {
        stop(Duration.ofSeconds(30));
    }

    /**
     * Installs an extension: it registers its hooks with this engine, through a context of its own
     * that takes hooks only while it installs. Only while the engine is built.
     */
    void install(Extension extension) {
        if (prepared || started) {
            throw new IllegalStateException("Extensions are installed while the engine is built");
        }
        String name = Objects.requireNonNull(extension.name(), "name");
        if (extensions.stream().anyMatch(installed -> installed.name().equals(name))) {
            throw new IllegalArgumentException("An extension named " + name + " is installed already: a feature installed twice "
                + "usually counts twice. Give one of them another name() if both are meant");
        }
        Context context = new Context(name);
        try {
            extension.install(context);
        } finally {
            context.installing = false;
        }
        extensions.add(extension);
    }

    /** Extension settings by topic name, then setting name; see {@link SettingKey}. */
    private Map<String, Map<String, Object>> extensionSettings = Map.of();
    /** Engine-wide extension settings by name, every declared one, and where each value came from. */
    private Map<String, Object> engineSettingValues = Map.of();
    private Map<String, EffectiveSettings.Setting> engineSettings = Map.of();

    TaskEngine withExtensionSettings(Map<String, Map<String, Object>> settings) {
        Map<String, Map<String, Object>> copy = new LinkedHashMap<>();
        settings.forEach((topic, values) -> copy.put(topic, Map.copyOf(values)));
        this.extensionSettings = Map.copyOf(copy);
        return this;
    }

    TaskEngine withEngineSettings(Map<String, Object> values, Map<String, EffectiveSettings.Setting> report) {
        this.engineSettingValues = Map.copyOf(values);
        this.engineSettings = Collections.unmodifiableMap(new TreeMap<>(report));
        return this;
    }

    /**
     * The installed extensions' engine-wide settings, by name, with where each value came from;
     * see {@link Extension#engineSettings()}. Their per-topic settings are in {@link #effectiveSettings()}.
     */
    public Map<String, EffectiveSettings.Setting> engineSettings() {
        return engineSettings;
    }

    /** The topic with its handler wrapped by every decorator, the first outermost. */
    private <T> Topic<T> decorated(Topic<T> topic) {
        BatchHandler<T> handler = topic.handler();
        for (HandlerDecorator decorator : decorators.reversed()) {
            handler = Objects.requireNonNull(decorator.decorate(topic.ref(), handler), "a decorator returned no handler");
        }
        return new Topic<>(topic.ref(), handler, topic.codec(), topic.settings(), topic.resources());
    }

    /** Asks every extension channel's reloads to run soon, as anything may have changed. */
    private void reloadChannels() {
        channels.values().forEach(reloads -> reloads.forEach(ChannelReload::everything));
    }

    /** Runs the extensions' start actions, in the order they were registered; the first that throws fails the start. */
    private void startExtensions() {
        for (Runnable action : onStart) {
            action.run();
        }
    }

    /** Runs the extensions' stop actions; one that throws is logged, and the others still run. */
    private void stopExtensions() {
        for (Runnable action : onStop) {
            try {
                action.run();
            } catch (RuntimeException e) {
                LOGGER.warn("An extension's stop action failed; the engine stops anyway", e);
            }
        }
    }

    /** The names of the installed extensions, in the order they were installed. */
    public List<String> extensions() {
        return extensions.stream().map(Extension::name).toList();
    }

    /**
     * The installed extension of this type, the first one if there are several: for code that has
     * the engine but not the extension, such as an operator's tool.
     */
    public <E extends Extension> Optional<E> extension(Class<E> type) {
        return extensions.stream().filter(type::isInstance).map(type::cast).findFirst();
    }

    /** What the installed extensions show operators, in the order they registered it; see {@link OperatorView}. */
    public List<OperatorView> operatorViews() {
        return List.copyOf(views.values());
    }

    /** The context one extension installs into: this engine's. */
    private final class Context implements ExtensionContext {

        private final String extension;
        /** Hooks are taken only while the extension installs. */
        private volatile boolean installing = true;

        Context(String extension) {
            this.extension = extension;
        }

        private void hook(String what) {
            if (!installing) {
                throw new IllegalStateException(extension + " registered " + what + " after it was installed: an extension "
                    + "registers its hooks in install()");
            }
        }

        @Override
        public TaskStore store() {
            return store;
        }

        @Override
        public <C> C capability(Class<C> type) {
            for (StoreCapability provider : ServiceLoader.load(StoreCapability.class, type.getClassLoader())) {
                Optional<C> found = provider.create(type, store);
                if (found.isPresent()) {
                    return type.cast(found.get());
                }
            }
            throw new IllegalStateException("No " + type.getName() + " for " + store + ": add the module that provides it for this store");
        }

        @Override
        public Tasks tasks() {
            return tasks;
        }

        @Override
        public TaskWriter tasks(StoreTransaction transaction) {
            return tasks.in(transaction);
        }

        @Override
        public TaskAdmin admin() {
            return admin;
        }

        @Override
        public InstantSource clock() {
            return store.clock();
        }

        @Override
        public List<TopicRef<?>> topics() {
            return TaskEngine.this.topics.stream().<TopicRef<?>>map(Topic::ref).toList();
        }

        @Override
        public <R extends @Nullable Object> R transact(String what, Work<R> work) {
            Objects.requireNonNull(work, "work");
            return Unchecked.call(what, () -> EngineCore.transact(store, work::run));
        }

        @Override
        public <T> Optional<T> topicSetting(TopicRef<?> topic, SettingKey<T> key) {
            if (!topicsByName.containsKey(topic.name())) {
                return Optional.empty();
            }
            Object value = extensionSettings.getOrDefault(topic.name(), Map.of()).get(key.name());
            return Optional.of(value == null ? key.defaultValue() : key.checked(value));
        }

        @Override
        public <T> T setting(SettingKey<T> key) {
            Object value = engineSettingValues.get(key.name());
            if (value == null) {
                throw new IllegalArgumentException("No installed extension declares the engine setting " + key.name()
                    + " in engineSettings()");
            }
            return key.checked(value);
        }

        @Override
        public boolean leads(String job) {
            Housekeeper keeper = housekeeper;
            return started && keeper != null && keeper.leads(job);
        }

        @Override
        public void runSoon(String job) {
            if (extensionJobs.stream().noneMatch(each -> each.name().equals(job))) {
                throw new IllegalArgumentException("This engine has no extension job named '" + job + "'");
            }
            Housekeeper keeper = housekeeper;
            if (started && keeper != null) {
                keeper.runSoon(job);
            }
        }

        @Override
        public void wake(TopicRef<?> topic) {
            TaskEngine.this.wake(topic.name());
        }

        @Override
        public void wake(String topic) {
            TaskEngine.this.wake(Objects.requireNonNull(topic, "topic"));
        }

        @Override
        public void claimGate(ClaimGate gate) {
            hook("a claim gate");
            gates.add(new NamedGate(extension, Objects.requireNonNull(gate, "gate")));
        }

        @Override
        public void batchListener(BatchListener listener) {
            hook("a batch listener");
            batchListeners.add(Objects.requireNonNull(listener, "listener"));
        }

        @Override
        public void handlerDecorator(HandlerDecorator decorator) {
            hook("a handler decorator");
            decorators.add(Objects.requireNonNull(decorator, "decorator"));
        }

        @Override
        public void job(HousekeepingJob job) {
            hook("a job");
            Objects.requireNonNull(job, "job");
            if (Housekeeper.RESERVED.contains(job.name())) {
                throw new IllegalArgumentException("The engine has a job or lease named '" + job.name() + "' of its own: name the job after the extension");
            }
            if (extensionJobs.stream().anyMatch(other -> other.name().equals(job.name()))) {
                throw new IllegalArgumentException("Another job is named '" + job.name() + "': each job of an engine has a name of its own");
            }
            extensionJobs.add(job);
        }

        @Override
        public void channel(String name, Runnable reload) {
            Objects.requireNonNull(reload, "reload");
            channel(name, payloads -> reload.run());
        }

        @Override
        public void channel(String name, Reload reload) {
            hook("a channel");
            Objects.requireNonNull(reload, "reload");
            if (!Notifier.CHANNEL.matcher(name).matches() || name.equals("ready") || name.equals(Notifier.TOPIC)) {
                throw new IllegalArgumentException("Invalid channel '" + name + "': a lowercase word other than ready and topic");
            }
            channels.computeIfAbsent(name, ignored -> new ArrayList<>()).add(new ChannelReload(name, reload));
        }

        @Override
        public void operatorView(OperatorView view) {
            hook("an operator view");
            Objects.requireNonNull(view, "view");
            String name = view.name();
            if (name == null || !OperatorView.NAME.matcher(name).matches()) {
                throw new IllegalArgumentException("Invalid operator view name '" + name + "': lowercase words with hyphens, such as schedules");
            }
            if (views.putIfAbsent(name, view) != null) {
                throw new IllegalArgumentException("Another operator view is named '" + name + "'");
            }
        }

        @Override
        public void observer(TaskObserver added) {
            hook("an observer");
            observer = TaskObserver.composite(List.of(observer, Objects.requireNonNull(added, "observer")));
            tasks.taskObserver(observer);
        }

        @Override
        public void onStart(Runnable action) {
            hook("a start action");
            onStart.add(Objects.requireNonNull(action, "action"));
        }

        @Override
        public void onStop(Runnable action) {
            hook("a stop action");
            onStop.add(Objects.requireNonNull(action, "action"));
        }

        @Override
        public String toString() {
            return "ExtensionContext[" + extension + ", " + TaskEngine.this + "]";
        }
    }

    @Override
    public String toString() {
        return "TaskEngine[" + settings.nodeName() + ", " + store + "]";
    }
}
