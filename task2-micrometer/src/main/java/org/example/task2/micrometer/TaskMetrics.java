package org.example.task2.micrometer;

import org.example.task2.admin.QueueCounts;
import org.example.task2.admin.TopicSnapshot;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToDoubleFunction;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.example.task2.engine.TaskEngine;

/**
 * Queue gauges per topic, from the counts the housekeeping leader snapshots (every minute by
 * default), so a scrape never counts the queue. Snapshots are read at most once per
 * {@code refresh}, and gauges for topics created at runtime appear with the first read that
 * sees them.
 *
 * <ul>
 *   <li>{@value #TOPICS}: how many topics the last snapshot has. It is there from the start, so
 *       every scrape reads the snapshots and finds new topics, also when the application starts
 *       before the first snapshot is taken.</li>
 *   <li>{@value #QUEUE}, by topic and state: scheduled, retrying, ready, active, suspended,
 *       failed (dead letters), and due (delayed or retrying tasks whose time has come).</li>
 *   <li>{@value #OLDEST_READY}: seconds since the oldest ready task was pushed, 0 if none is
 *       ready. The most useful thing to alert on: it grows when a topic stops keeping up, however
 *       fast it was going before.</li>
 *   <li>{@value #FAILING}: unfinished tasks whose last run failed, dead letters included, and
 *       {@value #OLDEST_FAILING}: seconds since the longest-failing of them first failed, 0 if
 *       none is. Alert on this one rather than on dead letters, which may take days to come.</li>
 *   <li>{@value #SNAPSHOT_AGE}: seconds since the counts were taken. If it grows past a few
 *       snapshot intervals, no node is running housekeeping.</li>
 * </ul>
 */
public final class TaskMetrics implements MeterBinder {

    public static final String TOPICS = "task.queue.topics";
    public static final String QUEUE = "task.queue";
    public static final String OLDEST_READY = "task.queue.oldest.ready";
    public static final String FAILING = "task.queue.failing";
    public static final String OLDEST_FAILING = "task.queue.oldest.failing";
    public static final String SNAPSHOT_AGE = "task.queue.snapshot.age";

    private static final System.Logger LOGGER = System.getLogger(TaskMetrics.class.getName());
    private static final Map<String, ToDoubleFunction<QueueCounts>> STATES = Map.of(
        "scheduled", QueueCounts::scheduled,
        "retrying", QueueCounts::retrying,
        "ready", QueueCounts::ready,
        "active", QueueCounts::active,
        "suspended", QueueCounts::suspended,
        "failed", QueueCounts::failed,
        "due", QueueCounts::due);

    private final Callable<List<TopicSnapshot>> snapshots;
    private final Duration refresh;
    private final InstantSource clock;
    private final Set<String> registered = ConcurrentHashMap.newKeySet();
    private volatile Map<String, TopicSnapshot> latest = Map.of();
    private volatile Instant readAt;
    private volatile MeterRegistry registry;

    /** Reads the engine's snapshots at most every 10 seconds. */
    public TaskMetrics(TaskEngine engine) {
        this(engine.admin()::snapshots, Duration.ofSeconds(10), InstantSource.system());
    }

    public TaskMetrics(Callable<List<TopicSnapshot>> snapshots, Duration refresh, InstantSource clock) {
        this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
        this.refresh = Objects.requireNonNull(refresh, "refresh");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        this.registry = registry;
        Gauge.builder(TOPICS, this, metrics -> {
                metrics.refresh(false);
                return metrics.latest.size();
            })
            .description("Topics in the last queue snapshot")
            .register(registry);
        refresh(true);
    }

    private TopicSnapshot snapshot(String topic) {
        refresh(false);
        return latest.get(topic);
    }

    private synchronized void refresh(boolean now) {
        Instant time = clock.instant();
        if (!now && readAt != null && Duration.between(readAt, time).compareTo(refresh) < 0) {
            return;
        }
        readAt = time;
        try {
            Map<String, TopicSnapshot> read = new HashMap<>();
            snapshots.call().forEach(snapshot -> read.put(snapshot.topic(), snapshot));
            latest = Map.copyOf(read);
        } catch (Exception e) {
            LOGGER.log(System.Logger.Level.WARNING, "Could not read the queue snapshots; the gauges keep the last ones", e);
        }
        latest.keySet().stream().filter(registered::add).forEach(this::register);
    }

    private void register(String topic) {
        STATES.forEach((state, value) -> Gauge.builder(QUEUE, this, metrics -> metrics.count(topic, value))
            .description("Unfinished tasks, from the last snapshot")
            .baseUnit("tasks")
            .tag("topic", topic)
            .tag("state", state)
            .register(registry));
        Gauge.builder(OLDEST_READY, this, metrics -> metrics.age(topic, snapshot -> snapshot.counts().oldestReadyAt()))
            .description("Seconds since the oldest ready task was pushed, from the last snapshot; 0 if none is ready")
            .baseUnit("seconds")
            .tag("topic", topic)
            .register(registry);
        Gauge.builder(FAILING, this, metrics -> metrics.count(topic, QueueCounts::failing))
            .description("Unfinished tasks whose last run failed, from the last snapshot")
            .baseUnit("tasks")
            .tag("topic", topic)
            .register(registry);
        Gauge.builder(OLDEST_FAILING, this, metrics -> metrics.age(topic, snapshot -> snapshot.counts().oldestFailingAt()))
            .description("Seconds since the longest-failing task first failed, from the last snapshot; 0 if none is failing")
            .baseUnit("seconds")
            .tag("topic", topic)
            .register(registry);
        Gauge.builder(SNAPSHOT_AGE, this, metrics -> metrics.age(topic, TopicSnapshot::takenAt))
            .description("Seconds since the queue counts were snapshotted")
            .baseUnit("seconds")
            .tag("topic", topic)
            .register(registry);
    }

    private double count(String topic, ToDoubleFunction<QueueCounts> value) {
        TopicSnapshot snapshot = snapshot(topic);
        return snapshot == null ? Double.NaN : value.applyAsDouble(snapshot.counts());
    }

    private double age(String topic, java.util.function.Function<TopicSnapshot, Instant> since) {
        TopicSnapshot snapshot = snapshot(topic);
        if (snapshot == null) {
            return Double.NaN;
        }
        Instant from = since.apply(snapshot);
        return from == null ? 0 : Math.max(0, Duration.between(from, clock.instant()).toMillis() / 1000.0);
    }
}
