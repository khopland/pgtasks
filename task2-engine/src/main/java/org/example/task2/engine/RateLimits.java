package org.example.task2.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.example.task2.spi.TaskStore;
import org.example.task2.spi.TaskStore.LimitOverride;

/**
 * The rate limits of this node's topics, and operators' overrides of them (plan section 08).
 * Overrides live in the store, which notifies every node of a change, so each applies it within
 * about a second; every heartbeat reloads them too, in case a notification was lost. An override
 * can lapse by itself, or be reset to the configured values. Each change bumps the limit's
 * version, and a change can name the version it is based on, so two operators can't overwrite
 * each other.
 */
public final class RateLimits {

    private static final System.Logger LOGGER = System.getLogger(RateLimits.class.getName());

    /**
     * One limit as this node sees it.
     *
     * @param configured what this node's configuration says; null if none of its topics use it
     * @param effective  what claims use now: the configured values with an active override
     *                   applied; null if none of this node's topics use it
     * @param override   the last override, active, lapsed or reset; null if never overridden
     * @param version    pass this with the next change; 0 if never overridden
     */
    public record Limit(String name, RateLimit configured, RateLimit effective, LimitOverride override, long version) {
    }

    private final TaskStore store;
    private final Map<String, RateLimit> configured = new TreeMap<>();
    private final Consumer<String> changed;
    private final AtomicBoolean refreshPending = new AtomicBoolean();
    private volatile Map<String, LimitOverride> overrides = Map.of();
    private Map<String, RateLimit> lastEffective = Map.of();

    /** @param changed told the name of each limit whose effective values changed */
    RateLimits(TaskStore store, Collection<RateLimit> configured, Consumer<String> changed) {
        this.store = store;
        configured.forEach(limit -> this.configured.putIfAbsent(limit.name(), limit));
        this.changed = changed;
    }

    /** The limits this node's topics use, by name. */
    public List<Limit> list() {
        return configured.keySet().stream().map(name -> limit(name, overrides.get(name))).toList();
    }

    /** A limit this node uses or someone has overridden. */
    public Optional<Limit> find(String name) {
        LimitOverride override = overrides.get(name);
        if (!configured.containsKey(name) && override == null) {
            return Optional.empty();
        }
        return Optional.of(limit(name, override));
    }

    /**
     * Overrides a limit on every node.
     *
     * @param rate      the rate, or null to keep the configured one
     * @param burst     the burst, or null to keep the configured one
     * @param lasting   how long until the override lapses by itself, or null for until reset
     * @param by        who made the change, for the record
     * @param ifVersion the version this change is based on, or null to change it regardless
     * @throws VersionConflictException if the limit's version isn't {@code ifVersion}
     * @throws IllegalArgumentException if no node has configured the limit
     */
    public Limit override(String name, Rate rate, Integer burst, Duration lasting, String by, Long ifVersion) throws Exception {
        Objects.requireNonNull(name, "name");
        if (rate == null && burst == null) {
            throw new IllegalArgumentException("An override sets a rate, a burst or both; use reset(...) to go back to the configured values");
        }
        if (lasting != null && !lasting.isPositive()) {
            throw new IllegalArgumentException("An override must last a positive time");
        }
        Instant until = lasting == null ? null : store.clock().instant().plus(lasting);
        return change(name, rate == null ? null : rate.perSecond(), burst, until, by, ifVersion);
    }

    /**
     * Resets a limit to its configured values on every node.
     *
     * @throws VersionConflictException if the limit's version isn't {@code ifVersion}
     */
    public Limit reset(String name, String by, Long ifVersion) throws Exception {
        return change(Objects.requireNonNull(name, "name"), null, null, null, by, ifVersion);
    }

    private Limit change(String name, Double perSecond, Integer burst, Instant until, String by, Long ifVersion) throws Exception {
        Optional<LimitOverride> changedOverride = EngineCore.transact(store, transaction ->
            store.overrideLimit(transaction, name, perSecond, burst, until, by, ifVersion));
        if (changedOverride.isEmpty()) {
            throw new VersionConflictException("Rate limit " + name + " has changed since version " + ifVersion + "; read it again and retry");
        }
        // The notification reaches this node too, but don't wait for it.
        refresh();
        return limit(name, changedOverride.get());
    }

    /** The values claims use now for a configured limit. */
    RateLimit effective(RateLimit limit) {
        return apply(limit, overrides.get(limit.name()), store.clock().instant());
    }

    private Limit limit(String name, LimitOverride override) {
        RateLimit limit = configured.get(name);
        RateLimit effective = limit == null ? null : apply(limit, override, store.clock().instant());
        return new Limit(name, limit, effective, override, override == null ? 0 : override.version());
    }

    private static RateLimit apply(RateLimit limit, LimitOverride override, Instant now) {
        if (override == null || !override.activeAt(now)) {
            return limit;
        }
        return limit.withRate(override.perSecond() != null ? override.perSecond() : limit.perSecond(),
            override.burst() != null ? override.burst() : limit.burst());
    }

    /**
     * Reloads the overrides and reports the limits whose effective values changed, including
     * overrides that lapsed since the last time.
     */
    synchronized void refresh() throws Exception {
        Map<String, LimitOverride> loaded = new HashMap<>();
        EngineCore.transact(store, store::limitOverrides).forEach(override -> loaded.put(override.name(), override));
        overrides = Map.copyOf(loaded);
        Instant now = store.clock().instant();
        Map<String, RateLimit> effective = new LinkedHashMap<>();
        configured.forEach((name, limit) -> effective.put(name, apply(limit, loaded.get(name), now)));
        Map<String, RateLimit> previous = lastEffective;
        lastEffective = effective;
        effective.forEach((name, limit) -> {
            RateLimit before = previous.get(name);
            if (before != null && !before.equals(limit)) {
                LimitOverride override = loaded.get(name);
                LOGGER.log(System.Logger.Level.INFO, "Rate limit " + name + " is now " + Rate.perSecond(limit.perSecond()) + ", burst " + limit.burst()
                    + (limit.equals(configured.get(name)) ? ", as configured" : ", overridden by " + override.updatedBy()
                    + (override.until() == null ? "" : " until " + override.until())));
                changed.accept(name);
            }
        });
    }

    /** Reloads the overrides soon, on another thread: for notifications, which mustn't wait on the store. */
    void refreshSoon() {
        if (refreshPending.compareAndSet(false, true)) {
            Thread.ofVirtual().name("task2-limits").start(() -> {
                refreshPending.set(false);
                try {
                    refresh();
                } catch (Exception e) {
                    LOGGER.log(System.Logger.Level.WARNING, "Could not reload the rate limit overrides; the next heartbeat tries again", e);
                }
            });
        }
    }

    @Override
    public String toString() {
        return "RateLimits" + list();
    }
}
