package io.github.khopland.pgtasks.engine;

import java.util.List;
import java.util.Map;

import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TopicRef;

import org.jspecify.annotations.Nullable;

/**
 * Sees what the engine does, for metrics, tracing and logging context, without the engine
 * depending on any of them. {@code pgtasks-micrometer} implements it with Micrometer.
 *
 * <p>Counts are only reported for committed changes: {@link #pushed} once the push's transaction
 * has committed, {@link BatchObservation#completed} once the batch's completion has. Every method
 * runs on the thread doing the work, so it must be quick. A method that throws is logged and
 * otherwise ignored.
 */
public interface TaskObserver {

    TaskObserver NONE = new TaskObserver() {
    };

    /**
     * Every one of these observers, in order: each sees everything, and one that throws doesn't
     * stop the others. A push stores the first trace context any of them gives.
     */
    static TaskObserver composite(java.util.List<? extends TaskObserver> observers) {
        java.util.List<TaskObserver> all = observers.stream().filter(observer -> observer != NONE).<TaskObserver>map(observer -> observer).toList();
        return all.isEmpty() ? NONE : all.size() == 1 ? all.getFirst() : new CompositeObserver(all);
    }

    /**
     * Tasks are about to be written to a topic, inside the pushing transaction. The returned
     * observation's {@link PushObservation#traceContext()} is stored with each task, and is closed
     * once the write is done, whether the transaction commits or not.
     */
    default PushObservation pushing(TopicRef<?> topic, int tasks) {
        return PushObservation.NONE;
    }

    /** A push has committed, with one result per creation. */
    default void pushed(TopicRef<?> topic, List<Pushed> results) {
    }

    /**
     * A claimed batch is about to run. The engine opens the observation on the thread running the
     * handler and again on the thread completing the batch, so downstream pushes see it as the
     * current trace, and closes it once the batch is done.
     */
    default BatchObservation batch(TopicRef<?> topic, List<Task<?>> tasks) {
        return BatchObservation.NONE;
    }

    /** This node's breaker for a topic opened or closed. */
    default void breaker(TopicRef<?> topic, boolean open) {
    }

    /**
     * A claim found the topic's rate limit spent: the topic waits this long, or until the limit
     * changes, before it claims again on this node.
     */
    default void limitWait(TopicRef<?> topic, String limit, java.time.Duration wait) {
    }

    /** What became of a task when its batch completed. */
    enum Result {
        SUCCEEDED, FILTERED, SUSPENDED, DELAYED, THROTTLED,
        /** Failed without using up an attempt: {@code Decision.unavailable}, or a type the retry policy doesn't count. */
        UNAVAILABLE,
        RETRYING, DEAD_LETTERED
    }

    /** Something made current on a thread until closed. */
    interface Scope extends AutoCloseable {

        Scope NONE = () -> {
        };

        @Override
        void close();
    }

    interface PushObservation extends AutoCloseable {

        PushObservation NONE = new PushObservation() {
            @Override
            public @Nullable String traceContext() {
                return null;
            }

            @Override
            public void close() {
            }
        };

        /** The pushing code's trace as a W3C {@code traceparent}, or null. */
        @Nullable String traceContext();

        @Override
        void close();
    }

    interface BatchObservation {

        BatchObservation NONE = new BatchObservation() {
        };

        /** Makes the batch current on this thread until the scope is closed. */
        default Scope open() {
            return Scope.NONE;
        }

        /**
         * The handler threw, timed out, or the completion failed. Called at most once, and may be
         * followed by {@link #completed} if the tasks' failure was recorded.
         */
        default void error(Throwable error) {
        }

        /** The batch's results committed: how many tasks ended up in each result. */
        default void completed(Map<Result, Integer> results) {
        }

        /** The batch was handed back unprocessed, because the engine is stopping or its claim was lost. */
        default void released() {
        }

        /** The batch is done. Called last, exactly once. */
        default void close() {
        }
    }
}
