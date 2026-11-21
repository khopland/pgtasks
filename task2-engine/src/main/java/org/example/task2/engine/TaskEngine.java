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
import org.example.task2.TaskStorageException;
import org.example.task2.Tasks;
import org.example.task2.TopicRef;
import org.example.task2.admin.DeadLetterQuery;
import org.example.task2.admin.FailureGroup;
import org.example.task2.admin.QueueCounts;
import org.example.task2.admin.Schedule;
import org.example.task2.admin.TaskView;
import org.example.task2.admin.TopicSnapshot;
import org.example.task2.spi.Notifier;
import org.example.task2.spi.TaskStore;

/**
 * Processes topics on this node. On start it lets the store check or migrate its schema,
 * registers the node, creates every topic registered by name, and starts one poll loop per topic
 * it runs, plus housekeeping.
 *
 * <p>Which topics it runs follows the store: every topic registered by name or matching one of its
 * patterns ({@link TaskRegistry#topicsMatching}), unless paused. A topic created, paused or resumed on any node is
 * broadcast, and every node also compares the store's topics with what it runs on each heartbeat,
 * so a lost notification delays a change by at most one heartbeat. A paused topic's running
 * batches finish; nothing is claimed from it until it is resumed.
 *
 * <p>Built with {@link #builder()}. Producers use {@link #tasks()}, operators {@link #admin()}
 * and {@link #limits()}. Every method throws only unchecked exceptions.
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
    private final TaskAdmin admin;
    private final Map<String, TopicWorker<?>> workersByName = new ConcurrentHashMap<>();
    private final Map<Integer, TopicWorker<?>> workersById = new ConcurrentHashMap<>();
    private final Map<Long, Integer> held = new ConcurrentHashMap<>();
    /** Guards starting and stopping workers. */
    private final Object topicsLock = new Object();
    /** Serializes reconciles, each from reading the topics to its last change. */
    private final Object reconcileLock = new Object();
    /** Workers of topics paused or gone, finishing their running batches, and the threads waiting for them. */
    private final Map<TopicWorker<?>, Thread> draining = new ConcurrentHashMap<>();
    private final Coalesced reconcileSoon = new Coalesced("topics", "topics", this::reconcileTopics);
    private EngineCore core;
    private Housekeeper housekeeper;
    private final RateLimits limits;
    private Notifier notifier;
    private volatile int nodeId = -1;
    private List<EffectiveSettings> effectiveSettings = List.of();
    private List<EventRoute<?>> eventRoutes = List.of();
    private List<Recurring<?>> recurring = List.of();
    private String version;
    private volatile boolean started;
    private boolean prepared;
    /** Whether it was started and stopped before, so a start must switch the reloads back on. */
    private boolean stopped;
    /** How long one retention run may take; shorter in tests. */
    Duration purgeBudget = Housekeeper.PURGE_BUDGET;

    TaskEngine(TaskStore store, EngineSettings settings, List<Topic<?>> topics) {
        this(store, settings, topics, List.of());
    }

    TaskEngine(TaskStore store, EngineSettings settings, List<Topic<?>> topics, List<TopicTemplate<?>> templates) {
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
        this.admin = new TaskAdmin(this, store);
        // Known from the configuration, so limits() works before the store is prepared.
        this.limits = new RateLimits(store, configuredLimits(), this::limitChanged);
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

    TaskEngine withEventRoutes(List<EventRoute<?>> routes) {
        this.eventRoutes = List.copyOf(routes);
        return this;
    }

    TaskEngine withRecurring(List<Recurring<?>> recurring) {
        this.recurring = List.copyOf(recurring);
        return this;
    }

    /** The recurring tasks this node's configurers define; see {@link TaskRegistry#recurring}. */
    public List<Recurring<?>> recurring() {
        return recurring;
    }

    /** What a topic's stored recurring tasks should be, by this node's configuration, with their payloads encoded. */
    @SuppressWarnings("unchecked")
    private <T> List<TaskStore.ScheduleSpec> scheduleSpecs(Topic<T> topic) {
        java.time.Instant now = store.clock().instant();
        List<TaskStore.ScheduleSpec> specs = new java.util.ArrayList<>();
        for (Recurring<?> each : recurring) {
            if (!each.topic().name().equals(topic.ref().name())) {
                continue;
            }
            Recurring<T> schedule = (Recurring<T>) each;
            specs.add(new TaskStore.ScheduleSpec(schedule.name(), schedule.identifier(), schedule.cron().expression(),
                schedule.cron().zone().getId(), schedule.insertion(),
                schedule.payload() == null ? null : topic.codec().encode(schedule.payload()), topic.codec().type(),
                schedule.cron().next(now)));
        }
        return specs;
    }

    /** Topics this node's modules push to, created when it prepares; see {@link TaskRegistry#pushesTo}. */
    private Set<String> pushedTo = Set.of();

    TaskEngine withPushedTo(java.util.Collection<String> topics) {
        this.pushedTo = Set.copyOf(topics);
        return this;
    }

    TaskEngine withVersion(String version) {
        this.version = version;
        return this;
    }

    TaskEngine withFallbackCodecs(PayloadCodecs codecs) {
        tasks.fallbackCodecs(codecs);
        return this;
    }

    TaskEngine withTopicsCreatedOnPush() {
        directory.createOnPush();
        return this;
    }

    /** The routes from application events to tasks; see {@link TaskRegistry#onEvent}. */
    public List<EventRoute<?>> eventRoutes() {
        return eventRoutes;
    }

    /**
     * Pushes the tasks the event routes make of an event: in the current transaction if there is
     * one, so they commit with it, or on their own if there isn't. Returns what was pushed, empty
     * if no route takes the event.
     */
    public List<org.example.task2.Pushed> publish(Object event) {
        List<org.example.task2.Pushed> pushed = new java.util.ArrayList<>();
        for (EventRoute<?> route : eventRoutes) {
            org.example.task2.Push<?> push = route.apply(event);
            if (push != null) {
                pushed.add(push(push));
            }
        }
        return pushed;
    }

    private <T> org.example.task2.Pushed push(org.example.task2.Push<T> push) {
        boolean inTransaction = settings.transactions().current().isPresent();
        org.example.task2.TaskWriter writer = inTransaction ? tasks : tasks.independently();
        return writer.push(push.topic(), push.insertion(), List.of(push.creation())).getFirst();
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

    /** The topics whose breaker is open on this node. */
    List<String> openBreakers() {
        return workersByName.values().stream().filter(TopicWorker::breakerOpen).map(worker -> worker.topic().ref().name()).sorted().toList();
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
     * recurring tasks, the topics its modules push to, and the rate limits. Enough for a node that only pushes, such as the
     * Spring starter's {@code task.enabled=false}: its pushes work whichever node starts first,
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
        List<RateLimit> configured = configuredLimits();
        for (Topic<?> topic : topics) {
            int id = EngineCore.transact(store, transaction -> {
                int created = store.createTopic(transaction, topic.ref().name());
                store.setRetention(transaction, created, topic.settings().retention());
                // The nodes that register a topic own its schedules: theirs replace whatever was there.
                store.syncSchedules(transaction, created, scheduleSpecs(topic));
                return created;
            });
            directory.register(topic.ref().name(), id);
        }
        for (String topic : pushedTo) {
            if (!topicsByName.containsKey(topic)) {
                directory.register(topic, EngineCore.transact(store, transaction -> store.createTopic(transaction, topic)));
            }
        }
        EngineCore.transact(store, transaction -> {
            for (RateLimit limit : configured) {
                store.ensureAdmission(transaction, limit.name(), limit.burst());
            }
            return null;
        });
        limits.refresh();
        prepared = true;
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
            // reloads that stop() switched off take notifications again, and the overrides
            // changed while this node was stopped are read now. It registers as a new node.
            reconcileSoon.restart();
            limits.resumeRefreshing();
            limits.refresh();
        }
        nodeId = EngineCore.transact(store, transaction -> store.registerNode(transaction, settings.nodeName(), version));
        core = new EngineCore(store, tasks, nodeId, new Semaphore(settings.maxInFlightBatches()), RandomGenerator.getDefault(), this::wake,
            held, () -> !started, limits, settings.observer());
        notifier = settings.notifier();
        housekeeper = null;
        try {
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
                reviveSweep();
            }, purgeBudget);
            housekeeper.start();
            reviveForVersion();
            started = true;
            reconcileTopics();
        } catch (Exception | Error e) {
            if (started) {
                stop(Duration.ZERO);
            } else {
                // Not started, so stop() would do nothing: end what this start got going.
                if (notifier != null) {
                    notifier.close();
                }
                if (housekeeper != null) {
                    housekeeper.stop();
                }
            }
            throw e;
        }
    }

    /**
     * Brings the workers in line with the store: starts a worker for every topic this node runs
     * that isn't paused, and stops the workers of topics paused or gone, letting their running
     * batches finish. On start, on every heartbeat, and when a topic changes anywhere.
     */
    void reconcileTopics() throws Exception {
        // One reconcile at a time, from its read to its last change: otherwise one that read the
        // topics before a resume committed could retire the worker that resume just started.
        synchronized (reconcileLock) {
            reconcileTopicsNow();
        }
    }

    private void reconcileTopicsNow() throws Exception {
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
     * How this node is doing: heartbeat, leadership, notifications and claims stuck past their
     * deadline. Reads this node's active claims, so it fails if the database can't be reached.
     */
    public EngineHealth health() {
        return Unchecked.call("Checking the engine's health", this::checkHealth);
    }

    private EngineHealth checkHealth() throws Exception {
        Duration term = settings.leaseTerm();
        if (!started) {
            return new EngineHealth(false, settings.nodeName(), nodeId, null, null, term, false, EngineHealth.Notifications.NONE, List.of(), List.of());
        }
        java.time.Instant now = store.clock().instant();
        java.time.Instant beat = housekeeper.lastHeartbeat();
        EngineHealth.Notifications notifications = notifier == null ? EngineHealth.Notifications.NONE
            : notifier.connected() ? EngineHealth.Notifications.CONNECTED : EngineHealth.Notifications.DISCONNECTED;
        List<EngineHealth.StuckClaim> stuck = new java.util.ArrayList<>();
        for (TaskStore.ActiveClaim claim : EngineCore.transact(store, transaction -> store.activeClaims(transaction, nodeId))) {
            TopicWorker<?> worker = workersById.get(claim.topicId());
            Duration allowed = (worker == null ? term : worker.topic().settings().batchDeadline()).plus(settings.heartbeat());
            Duration running = claim.claimedAt() == null ? Duration.ZERO : Duration.between(claim.claimedAt(), now);
            if (running.compareTo(allowed) > 0) {
                stuck.add(new EngineHealth.StuckClaim(worker == null ? directory.name(claim.topicId()) : worker.topic().ref().name(),
                    claim.taskId(), claim.attempt(), claim.claimedAt(), running));
            }
        }
        List<String> openBreakers = workersByName.values().stream().filter(TopicWorker::breakerOpen)
            .map(worker -> worker.topic().ref().name()).sorted().toList();
        return new EngineHealth(true, settings.nodeName(), nodeId, beat, beat == null ? null : Duration.between(beat, now), term,
            housekeeper.leading(), notifications, stuck, openBreakers);
    }

    /** The topics this node is processing now. */
    public Set<String> processing() {
        return started ? Set.copyOf(workersByName.keySet()) : Set.of();
    }

    /** The topic patterns this engine processes. */
    List<TopicTemplate<?>> templates() {
        return templates;
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
    java.util.Collection<TopicWorker<?>> workers() {
        return started ? workersByName.values() : List.of();
    }

    /** Heartbeats and renews or takes the housekeeping lease, now. */
    void heartbeatNow() throws Exception {
        if (started) {
            housekeeper.heartbeat();
        }
    }

    /**
     * The first node of a new application version revives the dead letters of the topics that ask
     * for it, once per version across the cluster: a fix that is deployed gets another go at what
     * failed before it. Nodes of the old version may still run some of them during a rolling deploy.
     */
    private void reviveForVersion() {
        if (version == null) {
            if (topics.stream().anyMatch(topic -> topic.settings().deadLetters().reviveOnNewVersion())) {
                LOGGER.log(System.Logger.Level.WARNING, "Some topics revive their dead letters on a new version, but the engine has no version; "
                    + "set it with TaskEngineBuilder.version(...) (the Spring starter takes it from BuildProperties)");
            }
            return;
        }
        for (Topic<?> topic : topics) {
            if (!topic.settings().deadLetters().reviveOnNewVersion()) {
                continue;
            }
            try {
                int id = admin.topicId(topic.ref().name());
                if (EngineCore.transact(store, transaction -> store.claimRevival(transaction, id, version))) {
                    int revived = admin.revive(topic.ref().name(), DeadLetterQuery.ALL);
                    LOGGER.log(System.Logger.Level.INFO, "Revived " + revived + " dead letter(s) of " + topic.ref() + " for version " + version);
                }
            } catch (Exception e) {
                LOGGER.log(System.Logger.Level.WARNING, "Could not revive the dead letters of " + topic.ref() + " for version " + version, e);
            }
        }
    }

    /** Revives the dead letters of topics that sweep them every so often, if it's time, once across the cluster. */
    private void reviveSweep() throws Exception {
        for (TopicWorker<?> worker : List.copyOf(workersByName.values())) {
            java.time.Duration every = worker.topic().settings().deadLetters().reviveEvery();
            if (every != null && EngineCore.transact(store, transaction -> store.claimReviveSweep(transaction, worker.topicId(), every))) {
                int revived = admin.revive(worker.topic().ref().name(), DeadLetterQuery.ALL);
                if (revived > 0) {
                    LOGGER.log(System.Logger.Level.INFO, "Revived " + revived + " dead letter(s) of " + worker.topic().ref() + ", as it does every " + every);
                }
            }
        }
    }

    /** If this node leads, runs expiry, promotion, retention and count snapshots once, now; and any dead-letter sweep due. */
    void runLeaderJobsNow() throws Exception {
        if (started) {
            reviveSweep();
            housekeeper.runLeaderJobsNow();
        }
    }

    /**
     * The rate limits of this node's topics, and operators' runtime overrides of them. Overrides
     * are read once the engine is prepared or started.
     */
    public RateLimits limits() {
        return limits;
    }

    private List<RateLimit> configuredLimits() {
        return Stream.concat(topics.stream().map(topic -> topic.settings().rateLimit()),
            templates.stream().map(template -> template.settings().rateLimit())).filter(Objects::nonNull).toList();
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
        stopped = true;
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

    @Override
    public String toString() {
        return "TaskEngine[" + settings.nodeName() + ", " + store + "]";
    }
}
