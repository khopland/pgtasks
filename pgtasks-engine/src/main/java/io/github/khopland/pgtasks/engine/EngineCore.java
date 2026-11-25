package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;

import io.github.khopland.pgtasks.engine.extension.BatchListener;
import io.github.khopland.pgtasks.engine.extension.ClaimGate;
import io.github.khopland.pgtasks.spi.StaleClaimException;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What every worker on this node shares. {@code held} maps the tasks this node is processing to
 * their attempt, so claims it lost track of can be found and released. {@code node} gives this
 * node's id, which changes if the cluster forgot the node and it registered again. {@code fenced}
 * says whether its heartbeat is so late that it must not start work (see {@link HeartbeatFence}).
 * {@code threads} makes the pollers' and batches' threads: virtual ones, except in the Fray tests,
 * whose scheduler can't drive virtual threads.
 */
record EngineCore(
    TaskStore store,
    DefaultTasks tasks,
    IntSupplier node,
    Semaphore inFlight,
    RandomGenerator random,
    Consumer<String> wake,
    Map<Long, Integer> held,
    BooleanSupplier stopping,
    TaskObserver observer,
    List<ClaimGate> gates,
    List<BatchListener> listeners,
    BooleanSupplier fenced,
    Supplier<Thread.Builder> threads
) {

    private static final Logger LOGGER = LoggerFactory.getLogger(EngineCore.class);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(5);

    @FunctionalInterface
    interface Work<T extends @Nullable Object> {
        T run(StoreTransaction transaction) throws Exception;
    }

    <T extends @Nullable Object> T transact(Work<T> work) throws Exception {
        return transact(store, work);
    }

    /** This node's id now. */
    int nodeId() {
        return node.getAsInt();
    }

    InstantSource clock() {
        return store.clock();
    }

    /**
     * Like {@link #transact}, but retries transient failures (a cut connection, a restarted server)
     * with backoff until the work succeeds, fails for another reason, or the engine stops. A
     * stale claim is never retried.
     */
    <T extends @Nullable Object> T transactRetrying(String what, Work<T> work) throws Exception {
        Duration backoff = Duration.ofMillis(100);
        while (true) {
            try {
                return transact(work);
            } catch (StaleClaimException e) {
                throw e;
            } catch (Exception e) {
                if (!Transient.is(e) || stopping.getAsBoolean()) {
                    throw e;
                }
                LOGGER.warn("{} failed on a transient error, retrying in {}: {}", what, backoff, e.toString());
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
                backoff = backoff.multipliedBy(2).compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : backoff.multipliedBy(2);
            }
        }
    }

    /** Runs the work in a new store transaction: committed if it returns, rolled back if it throws. */
    static <T extends @Nullable Object> T transact(TaskStore store, Work<T> work) throws Exception {
        try (StoreTransaction transaction = store.begin()) {
            T result = work.run(transaction);
            transaction.commit();
            return result;
        }
    }
}
