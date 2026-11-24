package io.github.khopland.pgtasks.ratelimit;

import io.github.khopland.pgtasks.engine.extension.ExtensionContext;

/**
 * Shared storage for {@link ClusterRateLimit}'s counters. A provider is found with
 * {@link ExtensionContext#capability(Class)}: add the module that provides it for the engine's
 * store, such as {@code pgtasks-rate-limit-postgres} for {@code pgtasks-postgres}.
 */
public interface SlidingWindowStore {

    /**
     * Atomically applies {@code action} to one topic's counter across every node and saves what it
     * leaves behind, if anything changed. The action is given the time to count by, in epoch
     * milliseconds, from one clock every node shares: the database's on Postgres, so nodes whose
     * own clocks drift still agree on where the window is. Called from a claim thread, so a provider
     * takes the store's lock without holding the engine's and keeps the transaction short.
     */
    <R> R update(String topic, Update<R> action);

    /** A change to one topic's counter at the shared time {@code now}. */
    @FunctionalInterface
    interface Update<R> {
        R apply(SlidingCounter counter, long now);
    }
}
