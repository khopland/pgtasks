package io.github.khopland.pgtasks.engine.extension;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.TaskObserver;

import org.jspecify.annotations.Nullable;

/**
 * Told what became of each task of a batch on this node, once its completion committed (or its
 * failure was recorded). Tasks handed back without running, as on a stop, aren't reported. Called
 * on the batch's thread: keep it quick.
 */
@FunctionalInterface
public interface BatchListener {

    /**
     * Whether a task failed, and whether the failure says anything about what its handler talks
     * to. A circuit breaker counts {@link #FAILED} and leaves {@link #TASK_FAULT} out.
     */
    enum Failure {
        /** It didn't fail: it finished, was delayed or suspended. */
        NONE,
        /**
         * It failed as it ran, or its batch's completion did, whether the attempt counted or not:
         * what a downstream system that is down looks like.
         */
        FAILED,
        /**
         * It failed through a fault of its own: the retry policy dead-letters the failure at once
         * ({@code Retry.deadLetterAt}), its payload couldn't be decoded, or it had no attempts left
         * and didn't run.
         */
        TASK_FAULT
    }

    /**
     * One task's result.
     *
     * @param taskId   the task, as {@link ClaimGate#claimed} named it
     * @param failure  whether it failed, and whose fault that was
     * @param error    what it failed with, if its decision had a cause: what the handler threw, or
     *                 what failed the batch's completion
     * @param duration from the claim to the completion, the same for every task of the batch
     */
    record Completed(long taskId, String identifier, int attempt, TaskObserver.Result result, Failure failure, @Nullable Throwable error,
                     Duration duration) {

        public Completed {
            Objects.requireNonNull(identifier, "identifier");
            Objects.requireNonNull(result, "result");
            Objects.requireNonNull(failure, "failure");
            Objects.requireNonNull(duration, "duration");
        }

        /**
         * A result whose failure follows from it ({@link Failure#FAILED} for a retry or a dead
         * letter, else none), without an error or a duration: for an extension's own tests.
         */
        public Completed(long taskId, String identifier, int attempt, TaskObserver.Result result) {
            this(taskId, identifier, attempt, result, failureOf(result), null, Duration.ZERO);
        }

        private static Failure failureOf(TaskObserver.Result result) {
            return switch (result) {
                case RETRYING, UNAVAILABLE, DEAD_LETTERED -> Failure.FAILED;
                case SUCCEEDED, FILTERED, SUSPENDED, DELAYED -> Failure.NONE;
            };
        }

        /** Whether it finished as it should: succeeded or filtered. */
        public boolean succeeded() {
            return result == TaskObserver.Result.SUCCEEDED || result == TaskObserver.Result.FILTERED;
        }
    }

    void completed(TopicRef<?> topic, List<Completed> tasks);
}
