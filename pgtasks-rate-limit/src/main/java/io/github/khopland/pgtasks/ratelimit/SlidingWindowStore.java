package io.github.khopland.pgtasks.ratelimit;

import java.util.function.Function;

import io.github.khopland.pgtasks.engine.extension.ExtensionContext;

/**
 * Shared storage for {@link ClusterRateLimit}'s counters. A provider is found with
 * {@link ExtensionContext#capability(Class)}: add the module that provides it for the engine's
 * store, such as {@code pgtasks-rate-limit-postgres} for {@code pgtasks-postgres}.
 */
public interface SlidingWindowStore {

    /**
     * Atomically applies {@code action} to one topic's counter across every node and saves what it
     * leaves behind. Called from a claim thread, so a provider takes the store's lock without
     * holding the engine's and keeps the transaction short.
     */
    <R> R update(String topic, Function<SlidingCounter, R> action);
}
