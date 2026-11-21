package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.InstantSource;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.random.RandomGenerator;

import io.github.khopland.pgtasks.spi.StaleClaimException;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

/**
 * What every worker on this node shares. {@code held} maps the tasks this node is processing to
 * their attempt, so claims it lost track of can be found and released.
 */
record EngineCore(
    TaskStore store,
    DefaultTasks tasks,
    int nodeId,
    Semaphore inFlight,
    RandomGenerator random,
    Consumer<String> wake,
    Map<Long, Integer> held,
    BooleanSupplier stopping,
    RateLimits limits,
    TaskObserver observer
) {

    private static final System.Logger LOGGER = System.getLogger(EngineCore.class.getName());
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(5);

    @FunctionalInterface
    interface Work<T> {
        T run(StoreTransaction transaction) throws Exception;
    }

    <T> T transact(Work<T> work) throws Exception {
        return transact(store, work);
    }

    InstantSource clock() {
        return store.clock();
    }

    /**
     * Like {@link #transact}, but retries transient failures (a cut connection, a restarted server)
     * with backoff until the work succeeds, fails for another reason, or the engine stops. A
     * stale claim is never retried.
     */
    <T> T transactRetrying(String what, Work<T> work) throws Exception {
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
                LOGGER.log(System.Logger.Level.WARNING, what + " failed on a transient error, retrying in " + backoff + ": " + e);
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
    static <T> T transact(TaskStore store, Work<T> work) throws Exception {
        try (StoreTransaction transaction = store.begin()) {
            T result = work.run(transaction);
            transaction.commit();
            return result;
        }
    }
}
