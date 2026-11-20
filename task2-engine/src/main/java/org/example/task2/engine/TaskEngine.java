package org.example.task2.engine;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.random.RandomGenerator;

import javax.sql.DataSource;

import org.example.task2.PayloadCodec;
import org.example.task2.Tasks;
import org.example.task2.spi.Notifier;
import org.example.task2.spi.TaskStore;

/**
 * Processes topics on this node. On start it registers the node, creates every registered
 * topic, and starts one poll loop per topic plus the heartbeat, expiry and promotion jobs.
 *
 * <p>Wiring is plain constructor arguments for now; the configuration model with defaults,
 * per-module configurers and overrides comes in a later phase.
 */
public final class TaskEngine implements AutoCloseable {

    private static final System.Logger LOGGER = System.getLogger(TaskEngine.class.getName());

    private final DataSource dataSource;
    private final TaskStore store;
    private final EngineSettings settings;
    private final List<Topic<?>> topics;
    private final Jdbc jdbc;
    private final TopicDirectory directory;
    private final DefaultTasks tasks;
    private final Map<String, TopicWorker<?>> workersByName = new ConcurrentHashMap<>();
    private final Map<Integer, TopicWorker<?>> workersById = new ConcurrentHashMap<>();
    private ScheduledExecutorService housekeeping;
    private Notifier notifier;
    private volatile int nodeId = -1;
    private volatile boolean started;

    public TaskEngine(DataSource dataSource, TaskStore store, EngineSettings settings, List<Topic<?>> topics) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.store = Objects.requireNonNull(store, "store");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.topics = List.copyOf(topics);
        Map<String, PayloadCodec<?>> codecs = new LinkedHashMap<>();
        for (Topic<?> topic : this.topics) {
            if (codecs.put(topic.ref().name(), topic.codec()) != null) {
                throw new IllegalArgumentException("Topic " + topic.ref() + " is registered twice");
            }
        }
        this.jdbc = new Jdbc(dataSource);
        this.directory = new TopicDirectory(store);
        this.tasks = new DefaultTasks(store, directory, codecs, settings.codecs(), settings.transactions(), jdbc, this::wake);
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
        nodeId = jdbc.transact(connection -> store.registerNode(connection, settings.nodeName(), version()));
        EngineCore core = new EngineCore(store, jdbc, tasks, nodeId, new Semaphore(settings.maxInFlightBatches()), RandomGenerator.getDefault(), this::wake);
        Map<Topic<?>, Integer> ids = new HashMap<>();
        for (Topic<?> topic : topics) {
            int id = jdbc.transact(connection -> {
                int created = store.createTopic(connection, topic.ref().name());
                if (topic.settings().rateLimit() != null) {
                    store.ensureAdmission(connection, topic.settings().rateLimit().name(), topic.settings().rateLimit().burst());
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
        housekeeping = Executors.newScheduledThreadPool(1, Thread.ofVirtual().name("task2-housekeeping-", 0).factory());
        schedule("heartbeat", settings.heartbeat(), () -> jdbc.transact(connection -> {
            store.heartbeat(connection, nodeId);
            return null;
        }));
        schedule("expiry", settings.heartbeat(), () -> jdbc.transact(connection -> store.requeueExpired(connection, nodeId, settings.expiry()))
            .forEach(this::wakeById));
        schedule("promotion", settings.promoteInterval(), () -> {
            List<Integer> promoted;
            do {
                promoted = jdbc.transact(connection -> store.promoteDue(connection, 1000));
                promoted.forEach(this::wakeById);
            } while (promoted.size() == 1000);
        });
        workersByName.values().forEach(TopicWorker::start);
        started = true;
        return this;
    }

    @FunctionalInterface
    private interface Job {
        void run() throws Exception;
    }

    private void schedule(String name, Duration interval, Job job) {
        housekeeping.scheduleWithFixedDelay(() -> {
            try {
                job.run();
            } catch (Exception e) {
                LOGGER.log(System.Logger.Level.WARNING, "The " + name + " job failed; it runs again in " + interval, e);
            }
        }, 0, interval.toNanos(), TimeUnit.NANOSECONDS);
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
            housekeeping.shutdownNow();
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
        return "TaskEngine[" + settings.nodeName() + ", " + dataSource + "]";
    }
}
