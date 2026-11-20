package org.example.task2.engine;

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

    BatchScope(String name) {
        this.name = name;
        this.executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name(name + "-", 0).factory());
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

    @Override
    public void close() {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                LOGGER.log(System.Logger.Level.WARNING, "Work in " + name + " ignored cancellation and is still running");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
