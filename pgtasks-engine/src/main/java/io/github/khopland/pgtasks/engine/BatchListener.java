package io.github.khopland.pgtasks.engine;

import java.util.List;

import io.github.khopland.pgtasks.TopicRef;

/**
 * Told what became of each task of a batch on this node, once its completion committed (or its
 * failure was recorded). Tasks handed back without running, as on a stop, aren't reported. Called
 * on the batch's thread: keep it quick.
 */
@FunctionalInterface
public interface BatchListener {

    /** One task's result. */
    record Completed(long taskId, String identifier, int attempt, TaskObserver.Result result) {
    }

    void completed(TopicRef<?> topic, List<Completed> tasks);
}
