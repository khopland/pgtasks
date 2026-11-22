package io.github.khopland.pgtasks.micrometer;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

import io.github.khopland.pgtasks.admin.QueueCounts;
import io.github.khopland.pgtasks.admin.TopicSnapshot;
import io.github.khopland.pgtasks.engine.RateLimit;
import io.github.khopland.pgtasks.engine.RateLimits;
import io.github.khopland.pgtasks.engine.TaskEngine;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Queue gauges per topic, from the counts the housekeeping leader snapshots (every minute by
 * default), so a scrape never counts the queue. Snapshots are read at most once per
 * {@code refresh}, and gauges for topics created at runtime appear with the first read that
 * sees them. Bound to an engine, it reads nothing until the engine has prepared the store (the
 * schema may not exist before), so the gauges are empty until then.
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
 *   <li>{@value #LIMIT_RATE} and {@value #LIMIT_BURST}, by limit and kind: the configured values
 *       and the effective ones, with an operator's override or a time window applied. Read from
 *       this node, without a query.</li>
 * </ul>
 */
public final class TaskMetrics implements MeterBinder {
    // Every gauge holds this binder strongly: Micrometer's default weak reference let an
    // instance bound by hand, without a reference kept, be collected, and its gauges read NaN.

    public static final String TOPICS = "pgtasks.queue.topics";
    public static final String QUEUE = "pgtasks.queue";
    public static final String OLDEST_READY = "pgtasks.queue.oldest.ready";
    public static final String FAILING = "pgtasks.queue.failing";
    public static final String OLDEST_FAILING = "pgtasks.queue.oldest.failing";
    public static final String SNAPSHOT_AGE = "pgtasks.queue.snapshot.age";
    public static final String LIMIT_RATE = "pgtasks.limit.rate";
    public static final String LIMIT_BURST = "pgtasks.limit.burst";

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskMetrics.class);
    private static final Map<String, ToDoubleFunction<QueueCounts>> STATES = Map.of(
        "scheduled", QueueCounts::scheduled,
        "retrying", QueueCounts::retrying,
        "ready", QueueCounts::ready,
        "active", QueueCounts::active,
        "suspended", QueueCounts::suspended,
        "failed", QueueCounts::failed,
        "due", QueueCounts::due);

    private final Callable<List<TopicSnapshot>> snapshots;
    private final Supplier<List<RateLimits.Limit>> limits;
    private final BooleanSupplier readable;
    private final Duration refresh;
    private final InstantSource clock;
    private final Set<String> registered = ConcurrentHashMap.newKeySet();
    private volatile Map<String, TopicSnapshot> latest = Map.of();
    private volatile @Nullable Instant readAt;
    private volatile @Nullable MeterRegistry registry;

    /** Reads the engine's snapshots at most every 10 seconds, once the engine has prepared the store. */
    public TaskMetrics(TaskEngine engine) {
        this(engine.admin()::snapshots, () -> engine.limits().list(), engine::prepared, Duration.ofSeconds(10), InstantSource.system());
    }

    public TaskMetrics(Callable<List<TopicSnapshot>> snapshots, Duration refresh, InstantSource clock) {
        this(snapshots, List::of, refresh, clock);
    }

    /** @param limits this node's rate limits, read on every scrape */
    public TaskMetrics(Callable<List<TopicSnapshot>> snapshots, Supplier<List<RateLimits.Limit>> limits, Duration refresh,
                       InstantSource clock) {
        this(snapshots, limits, () -> true, refresh, clock);
    }

    private TaskMetrics(Callable<List<TopicSnapshot>> snapshots, Supplier<List<RateLimits.Limit>> limits, BooleanSupplier readable,
                        Duration refresh, InstantSource clock) {
        this.readable = readable;
        this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
        this.limits = Objects.requireNonNull(limits, "limits");
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
            .strongReference(true)
            .register(registry);
        refresh(true);
        // The limits a node uses are its configuration's, known from the start.
        for (RateLimits.Limit limit : limits.get()) {
            for (String kind : List.of("configured", "effective")) {
                Gauge.builder(LIMIT_RATE, this, metrics -> metrics.limit(limit.name(), kind, RateLimit::perSecond))
                    .description("Claims per second the rate limit allows")
                    .baseUnit("tasks/s")
                    .tag("limit", limit.name())
                    .tag("kind", kind)
                    .strongReference(true)
                    .register(registry);
                Gauge.builder(LIMIT_BURST, this, metrics -> metrics.limit(limit.name(), kind, RateLimit::burst))
                    .description("Claims the rate limit allows at once")
                    .baseUnit("tasks")
                    .tag("limit", limit.name())
                    .tag("kind", kind)
                    .strongReference(true)
                    .register(registry);
            }
        }
    }

    private double limit(String name, String kind, ToDoubleFunction<RateLimit> value) {
        return limits.get().stream().filter(limit -> limit.name().equals(name)).findFirst()
            .flatMap(limit -> Optional.ofNullable(kind.equals("configured") ? limit.configured() : limit.effective()))
            .map(value::applyAsDouble).orElse(Double.NaN);
    }

    private @Nullable TopicSnapshot snapshot(String topic) {
        refresh(false);
        return latest.get(topic);
    }

    private synchronized void refresh(boolean now) {
        if (!readable.getAsBoolean()) {
            // Before the engine has prepared the store, its tables may not exist yet. Not marked as
            // read, so the first scrape after it has reads at once.
            return;
        }
        Instant time = clock.instant();
        Instant last = readAt;
        if (!now && last != null && Duration.between(last, time).compareTo(refresh) < 0) {
            return;
        }
        readAt = time;
        try {
            Map<String, TopicSnapshot> read = new HashMap<>();
            snapshots.call().forEach(snapshot -> read.put(snapshot.topic(), snapshot));
            latest = Map.copyOf(read);
        } catch (Exception e) {
            LOGGER.warn("Could not read the queue snapshots; the gauges keep the last ones", e);
        }
        latest.keySet().stream().filter(registered::add).forEach(this::register);
    }

    private void register(String topic) {
        MeterRegistry registry = Objects.requireNonNull(this.registry, "bindTo(registry) first");
        STATES.forEach((state, value) -> Gauge.builder(QUEUE, this, metrics -> metrics.count(topic, value))
            .description("Unfinished tasks, from the last snapshot")
            .baseUnit("tasks")
            .tag("topic", topic)
            .tag("state", state)
            .strongReference(true)
            .register(registry));
        Gauge.builder(OLDEST_READY, this, metrics -> metrics.age(topic, snapshot -> snapshot.counts().oldestReadyAt()))
            .description("Seconds since the oldest ready task was pushed, from the last snapshot; 0 if none is ready")
            .baseUnit("seconds")
            .tag("topic", topic)
            .strongReference(true)
            .register(registry);
        Gauge.builder(FAILING, this, metrics -> metrics.count(topic, QueueCounts::failing))
            .description("Unfinished tasks whose last run failed, from the last snapshot")
            .baseUnit("tasks")
            .tag("topic", topic)
            .strongReference(true)
            .register(registry);
        Gauge.builder(OLDEST_FAILING, this, metrics -> metrics.age(topic, snapshot -> snapshot.counts().oldestFailingAt()))
            .description("Seconds since the longest-failing task first failed, from the last snapshot; 0 if none is failing")
            .baseUnit("seconds")
            .tag("topic", topic)
            .strongReference(true)
            .register(registry);
        Gauge.builder(SNAPSHOT_AGE, this, metrics -> metrics.age(topic, TopicSnapshot::takenAt))
            .description("Seconds since the queue counts were snapshotted")
            .baseUnit("seconds")
            .tag("topic", topic)
            .strongReference(true)
            .register(registry);
    }

    private double count(String topic, ToDoubleFunction<QueueCounts> value) {
        TopicSnapshot snapshot = snapshot(topic);
        return snapshot == null ? Double.NaN : value.applyAsDouble(snapshot.counts());
    }

    private double age(String topic, Function<TopicSnapshot, @Nullable Instant> since) {
        TopicSnapshot snapshot = snapshot(topic);
        if (snapshot == null) {
            return Double.NaN;
        }
        Instant from = since.apply(snapshot);
        return from == null ? 0 : Math.max(0, Duration.between(from, clock.instant()).toMillis() / 1000.0);
    }
}
