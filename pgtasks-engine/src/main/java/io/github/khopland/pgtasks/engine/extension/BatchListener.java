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
     * One task's result. The engine makes these; an extension's tests make their own with the
     * constructor and the {@code with} methods.
     */
    final class Completed {

        private final long taskId;
        private final String identifier;
        private final int attempt;
        private final TaskObserver.Result result;
        private final Failure failure;
        private final @Nullable Throwable error;
        private final Duration duration;

        /**
         * A result whose failure follows from it ({@link Failure#FAILED} for a retry, an
         * unavailable or a dead letter, else none), without an error, taking no time.
         */
        public Completed(long taskId, String identifier, int attempt, TaskObserver.Result result) {
            this(taskId, identifier, attempt, result, failureOf(result), null, Duration.ZERO);
        }

        private Completed(long taskId, String identifier, int attempt, TaskObserver.Result result, Failure failure,
                          @Nullable Throwable error, Duration duration) {
            this.taskId = taskId;
            this.identifier = Objects.requireNonNull(identifier, "identifier");
            this.attempt = attempt;
            this.result = Objects.requireNonNull(result, "result");
            this.failure = Objects.requireNonNull(failure, "failure");
            this.error = error;
            this.duration = Objects.requireNonNull(duration, "duration");
        }

        private static Failure failureOf(TaskObserver.Result result) {
            return switch (result) {
                case RETRYING, UNAVAILABLE, DEAD_LETTERED -> Failure.FAILED;
                case SUCCEEDED, FILTERED, SUSPENDED, DELAYED -> Failure.NONE;
            };
        }

        /** This result, failed in this way and with this error, such as a dead letter that was the task's fault. */
        public Completed withFailure(Failure failure, @Nullable Throwable error) {
            return new Completed(taskId, identifier, attempt, result, failure, error, duration);
        }

        /** This result, having taken this long. */
        public Completed withDuration(Duration duration) {
            return new Completed(taskId, identifier, attempt, result, failure, error, duration);
        }

        /** The task, as {@link ClaimGate#claimed} named it. */
        public long taskId() {
            return taskId;
        }

        public String identifier() {
            return identifier;
        }

        /** The attempt the task was on when it was claimed. */
        public int attempt() {
            return attempt;
        }

        public TaskObserver.Result result() {
            return result;
        }

        /** Whether it failed, and whose fault that was. */
        public Failure failure() {
            return failure;
        }

        /**
         * What it failed with, if its decision had a cause: what the handler threw, or what failed
         * the batch's completion.
         */
        public @Nullable Throwable error() {
            return error;
        }

        /** From the claim to the completion, the same for every task of the batch. */
        public Duration duration() {
            return duration;
        }

        /** Whether it finished as it should: succeeded or filtered. */
        public boolean succeeded() {
            return result == TaskObserver.Result.SUCCEEDED || result == TaskObserver.Result.FILTERED;
        }

        @Override
        public boolean equals(@Nullable Object other) {
            return other instanceof Completed that && that.taskId == taskId && that.identifier.equals(identifier) && that.attempt == attempt
                && that.result == result && that.failure == failure && Objects.equals(that.error, error) && that.duration.equals(duration);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId, identifier, attempt, result, failure, error, duration);
        }

        @Override
        public String toString() {
            return "Completed[task " + taskId + " (" + identifier + "), attempt " + attempt + ", " + result + ", " + failure
                + (error == null ? "" : ", " + error) + ", " + duration + "]";
        }
    }

    void completed(TopicRef<?> topic, List<Completed> tasks);
}
