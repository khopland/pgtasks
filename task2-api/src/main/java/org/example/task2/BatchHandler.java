package org.example.task2;

/**
 * Handles a batch of claimed tasks and returns a decision for each of them, plus the downstream
 * work each one staged. The engine owns claims, fencing, retries and wake-ups.
 *
 * <p>If the handler throws, every task in the batch fails. A task the handler leaves out fails
 * too.
 */
@FunctionalInterface
public interface BatchHandler<T> {

    Decisions handle(Batch<T> batch) throws Exception;
}
