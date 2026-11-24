package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.random.RandomGenerator;
import java.util.stream.Collectors;

import io.github.khopland.pgtasks.BatchHandler;
import io.github.khopland.pgtasks.PayloadCodec;
import io.github.khopland.pgtasks.TaskStorageException;
import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.spi.Notifier;
import io.github.khopland.pgtasks.spi.StoreCapability;
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
 * topic deleted on any node is broadcast, and every node also compares the store's topics with what it runs on each heartbeat,
 * so a lost notification delays a change by at most one heartbeat. A deleted topic's running
 * batches finish; nothing is claimed from it until it is resumed.
 *
 * <p>Built with {@link #builder()}. Producers use {@link #tasks()}, operators {@link #admin()}
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
    private final List<String> extensions = new ArrayList<>();
    /** The builder's observers, and those installed extensions add. */
    private TaskObserver observer;
    /** Each extension channel's reloads, run soon on a message, a reconnect or a heartbeat. */
    private final Map<String, List<Coalesced>> channels = new LinkedHashMap<>();
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
            channels.values().forEach(reloads -> reloads.forEach(Coalesced::restart));
        }
        // Started before registering, which records the first heartbeat, so the fence errs early.
        HeartbeatFence fence = new HeartbeatFence(settings.heartbeat(), settings.expiry(), System::nanoTime);
        nodeId = EngineCore.transact(store, transaction -> store.registerNode(transaction, settings.nodeName(), version));
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
                            channels.getOrDefault(channel, List.of()).forEach(Coalesced::request);
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
            keeper.start();
            started = true;
            reconcileTopics();
            onStart.forEach(Runnable::run);
        } catch (Exception | Error e) {
            if (started) {
                stop(Duration.ZERO);
            } else {
                // Not started, so stop() would do nothing: end what this start got going.
                if (wakeUps != null) {
                    wakeUps.close();
                }
                Housekeeper keeper = housekeeper;
                if (keeper != null) {
                    keeper.stop();
                }
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
            return new EngineHealth(false, settings.nodeName(), nodeId, null, null, term, false, EngineHealth.Notifications.NONE, List.of());
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
            running(housekeeper).leading(), notifications, stuck);
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

    /** If this node leads, runs expiry, promotion, retention and count snapshots once, now. */
    void runLeaderJobsNow() throws Exception {
        if (started) {
            running(housekeeper).runLeaderJobsNow();
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
        int fresh = EngineCore.transact(store, transaction -> store.registerNode(transaction, settings.nodeName(), version));
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
     * and returns once every thread it started has ended. Threads that ignore cancellation get
     * {@link TopicWorker#PATIENCE} more, and are logged if they still run.
     */
    public synchronized void stop(Duration timeout) {
        if (!started) {
            return;
        }
        started = false;
        stopped = true;
        long deadline = System.nanoTime() + timeout.toNanos();
        for (Runnable action : onStop) {
            try {
                action.run();
            } catch (RuntimeException e) {
                LOGGER.warn("An extension's stop action failed; the engine stops anyway", e);
            }
        }
        try {
            // Nothing new starts: no wake-ups, no reloads.
            Notifier wakeUps = notifier;
            if (wakeUps != null) {
                wakeUps.close();
            }
            reconcileSoon.stop(TopicWorker.PATIENCE);
            for (List<Coalesced> reloads : channels.values()) {
                for (Coalesced reload : reloads) {
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
            for (TopicWorker<?> worker : all) {
                worker.awaitBatches(deadline);
            }
            for (Thread drain : drains) {
                drain.join(TopicWorker.PATIENCE);
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

    /** Installs an extension: it registers its hooks with this engine. Only while the engine is built. */
    void install(Extension extension) {
        if (prepared || started) {
            throw new IllegalStateException("Extensions are installed while the engine is built");
        }
        extension.install(new Context());
        extensions.add(extension.name());
    }

    /** Extension settings by topic name, then setting name; see {@link SettingKey}. */
    private Map<String, Map<String, Object>> extensionSettings = Map.of();

    TaskEngine withExtensionSettings(Map<String, Map<String, Object>> settings) {
        Map<String, Map<String, Object>> copy = new LinkedHashMap<>();
        settings.forEach((topic, values) -> copy.put(topic, Map.copyOf(values)));
        this.extensionSettings = Map.copyOf(copy);
        return this;
    }

    /** The topic with its handler wrapped by every decorator, the first outermost. */
    private <T> Topic<T> decorated(Topic<T> topic) {
        BatchHandler<T> handler = topic.handler();
        for (HandlerDecorator decorator : decorators.reversed()) {
            handler = Objects.requireNonNull(decorator.decorate(topic.ref(), handler), "a decorator returned no handler");
        }
        return new Topic<>(topic.ref(), handler, topic.codec(), topic.settings(), topic.resources());
    }

    /** Asks every extension channel's reloads to run soon. */
    private void reloadChannels() {
        channels.values().forEach(reloads -> reloads.forEach(Coalesced::request));
    }

    /** The names of the installed extensions, in the order they were installed. */
    public List<String> extensions() {
        return List.copyOf(extensions);
    }

    /** The context an extension installs into: this engine's. */
    private final class Context implements ExtensionContext {

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
        public InstantSource clock() {
            return store.clock();
        }

        @Override
        public boolean leading() {
            return leadsHousekeeping();
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
        public void claimGate(ClaimGate gate) {
            gates.add(Objects.requireNonNull(gate, "gate"));
        }

        @Override
        public void batchListener(BatchListener listener) {
            batchListeners.add(Objects.requireNonNull(listener, "listener"));
        }

        @Override
        public void handlerDecorator(HandlerDecorator decorator) {
            decorators.add(Objects.requireNonNull(decorator, "decorator"));
        }

        @Override
        public void job(HousekeepingJob job) {
            extensionJobs.add(Objects.requireNonNull(job, "job"));
        }

        @Override
        public void channel(String name, Runnable reload) {
            Objects.requireNonNull(reload, "reload");
            if (!Notifier.CHANNEL.matcher(name).matches() || name.equals("ready") || name.equals(Notifier.TOPIC)) {
                throw new IllegalArgumentException("Invalid channel '" + name + "': a lowercase word other than ready and topic");
            }
            channels.computeIfAbsent(name, ignored -> new ArrayList<>()).add(new Coalesced("channel-" + name, "channel " + name, reload::run));
        }

        @Override
        public void observer(TaskObserver added) {
            observer = TaskObserver.composite(List.of(observer, Objects.requireNonNull(added, "observer")));
            tasks.taskObserver(observer);
        }

        @Override
        public void onStart(Runnable action) {
            onStart.add(Objects.requireNonNull(action, "action"));
        }

        @Override
        public void onStop(Runnable action) {
            onStop.add(Objects.requireNonNull(action, "action"));
        }
    }

    @Override
    public String toString() {
        return "TaskEngine[" + settings.nodeName() + ", " + store + "]";
    }
}
