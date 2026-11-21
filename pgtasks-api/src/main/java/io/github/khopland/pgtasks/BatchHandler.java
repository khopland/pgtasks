package io.github.khopland.pgtasks;

import java.util.Objects;

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

    /**
     * Handles each task of the batch in turn with a per-task handler: a task whose handler returns
     * succeeds, one whose handler throws fails, and the others carry on.
     */
    static <T> BatchHandler<T> each(TaskHandler<T> handler) {
        Objects.requireNonNull(handler, "handler");
        return batch -> batch.decide(task -> {
            handler.handle(task);
            return Decision.success();
        });
    }
}
