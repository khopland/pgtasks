package org.example.task2.engine;

import java.time.InstantSource;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.random.RandomGenerator;

import org.example.task2.spi.StoreTransaction;
import org.example.task2.spi.TaskStore;

/** What every worker on this node shares. */
record EngineCore(
    TaskStore store,
    DefaultTasks tasks,
    int nodeId,
    Semaphore inFlight,
    RandomGenerator random,
    Consumer<String> wake
) {

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

    /** Runs the work in a new store transaction: committed if it returns, rolled back if it throws. */
    static <T> T transact(TaskStore store, Work<T> work) throws Exception {
        try (StoreTransaction transaction = store.begin()) {
            T result = work.run(transaction);
            transaction.commit();
            return result;
        }
    }
}
