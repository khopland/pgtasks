package io.github.khopland.pgtasks.engine;

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
import java.util.function.Consumer;

import io.github.khopland.pgtasks.admin.LimitOverride;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The rate limits of this node's topics, and operators' overrides of them (plan section 08).
 * Overrides live in the store, which notifies every node of a change, so each applies it within
 * about a second; every heartbeat reloads them too, in case a notification was lost. An override
 * can lapse by itself, or be reset to the configured values. Each change bumps the limit's
 * version, and a change can name the version it is based on, so two operators can't overwrite
 * each other.
 */
public final class RateLimits {

    private static final Logger LOGGER = LoggerFactory.getLogger(RateLimits.class);

    /**
     * One limit as this node sees it.
     *
     * @param configured what this node's configuration says; null if none of its topics use it
     * @param effective  what claims use now: the configured values with an active override
     *                   applied; null if none of this node's topics use it
     * @param override   the last override, active, lapsed or reset; null if never overridden
     * @param version    pass this with the next change; 0 if never overridden
     */
    public record Limit(String name, @Nullable RateLimit configured, @Nullable RateLimit effective, @Nullable LimitOverride override,
                        long version) {
    }

    private final TaskStore store;
    private final Map<String, RateLimit> configured = new TreeMap<>();
    private final Consumer<String> changed;
    private final Coalesced refreshSoon = new Coalesced("limits", "rate limit overrides", this::refresh);
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
    public Limit override(String name, @Nullable Rate rate, @Nullable Integer burst, @Nullable Duration lasting, @Nullable String by,
                          @Nullable Long ifVersion) {
        Objects.requireNonNull(name, "name");
        if (rate == null && burst == null) {
            throw new IllegalArgumentException("An override sets a rate, a burst or both; use reset(...) to go back to the configured values");
        }
        if (lasting != null && !lasting.isPositive()) {
            throw new IllegalArgumentException("An override must last a positive time");
        }
        Instant until = lasting == null ? null : store.clock().instant().plus(lasting);
        return Unchecked.call("Overriding rate limit " + name, () -> change(name, rate == null ? null : rate.perSecond(), burst, until, by, ifVersion));
    }

    /**
     * Resets a limit to its configured values on every node.
     *
     * @throws VersionConflictException if the limit's version isn't {@code ifVersion}
     */
    public Limit reset(String name, @Nullable String by, @Nullable Long ifVersion) {
        Objects.requireNonNull(name, "name");
        return Unchecked.call("Resetting rate limit " + name, () -> change(name, null, null, null, by, ifVersion));
    }

    private Limit change(String name, @Nullable Double perSecond, @Nullable Integer burst, @Nullable Instant until, @Nullable String by,
                         @Nullable Long ifVersion) throws Exception {
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

    private Limit limit(String name, @Nullable LimitOverride override) {
        RateLimit limit = configured.get(name);
        RateLimit effective = limit == null ? null : apply(limit, override, store.clock().instant());
        return new Limit(name, limit, effective, override, override == null ? 0 : override.version());
    }

    /** The configured limit at this time, its window applied, then an active override on top. */
    private static RateLimit apply(RateLimit configured, @Nullable LimitOverride override, Instant now) {
        RateLimit limit = configured.at(now);
        if (override == null || !override.activeAt(now)) {
            return limit;
        }
        Double perSecond = override.perSecond();
        Integer burst = override.burst();
        return limit.withRate(perSecond != null ? perSecond : limit.perSecond(), burst != null ? burst : limit.burst());
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
                String why = override != null && override.activeAt(now)
                    ? ", overridden by " + override.updatedBy() + (override.until() == null ? "" : " until " + override.until())
                    : Objects.requireNonNull(configured.get(name)).windows().isEmpty() ? ", as configured" : ", by its time window";
                LOGGER.info("Rate limit {} is now {}, burst {}{}", name, Rate.perSecond(limit.perSecond()), limit.burst(), why);
                changed.accept(name);
            }
        });
    }

    /** Reloads the overrides soon, on another thread: for notifications, which mustn't wait on the store. */
    void refreshSoon() {
        refreshSoon.request();
    }

    /** Reloads on notifications again, for an engine started again. */
    void resumeRefreshing() {
        refreshSoon.restart();
    }

    /** No more reloads on notifications; waits for one in progress. */
    void stopRefreshing(Duration patience) throws InterruptedException {
        refreshSoon.stop(patience);
    }

    @Override
    public String toString() {
        return "RateLimits" + list();
    }
}
