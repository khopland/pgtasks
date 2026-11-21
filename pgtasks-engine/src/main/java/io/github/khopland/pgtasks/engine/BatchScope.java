package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The scope of one batch. Work forked into it runs on virtual threads and cannot outlive the
 * batch: closing the scope cancels and joins whatever is left. Stands in for
 * StructuredTaskScope, which is still a preview API in JDK 25; never exposed publicly.
 */
final class BatchScope implements AutoCloseable {

    private static final System.Logger LOGGER = System.getLogger(BatchScope.class.getName());

    private final String name;
    private final ExecutorService executor;
    private final java.util.function.BooleanSupplier stopping;
    private volatile boolean abandoned;

    /** @param stopping whether the engine is stopping, when the scope stops waiting for work that ignores cancellation */
    BatchScope(String name, java.util.function.BooleanSupplier stopping) {
        this.name = name;
        this.stopping = stopping;
        this.executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name(name + "-", 0).factory());
    }

    /** Whether work in the scope ignored cancellation and still runs: its claims must not be handed back. */
    boolean abandoned() {
        return abandoned;
    }

    /** Runs the work in the scope and waits at most until the deadline, cancelling it after that. */
    <V> V call(Callable<V> work, Duration deadline) throws Exception {
        Future<V> future = executor.submit(work);
        try {
            return future.get(deadline.toNanos(), TimeUnit.NANOSECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw e;
        } catch (TimeoutException | InterruptedException e) {
            future.cancel(true);
            throw e;
        }
    }

    /**
     * Cancels what is left and waits until it has ended, however long that takes: work that
     * ignores cancellation keeps its tasks claimed, so a retry can't run next to it. Only an
     * engine that is stopping gives up waiting; the claims then stay until the node expires.
     */
    @Override
    public void close() {
        executor.shutdownNow();
        try {
            if (executor.awaitTermination(1, TimeUnit.SECONDS)) {
                return;
            }
            LOGGER.log(System.Logger.Level.WARNING, "Work in " + name + " ignored cancellation and is still running; its tasks stay claimed until it ends");
            long waited = 1;
            while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                waited++;
                if (stopping.getAsBoolean()) {
                    abandoned = true;
                    LOGGER.log(System.Logger.Level.WARNING, "Gave up waiting for " + name + " as the engine stops; its tasks stay claimed until this node expires");
                    return;
                }
                if (waited % 60 == 0) {
                    LOGGER.log(System.Logger.Level.WARNING, "Work in " + name + " still runs " + waited + " s after it was cancelled");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            abandoned = !executor.isTerminated();
        }
    }
}
