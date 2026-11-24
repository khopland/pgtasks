package io.github.khopland.pgtasks.engine;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import io.github.khopland.pgtasks.Batch;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TopicRef;

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
     * stop the others. A push stores the headers of all of them; for a name two of them give,
     * the first one's value.
     */
    static TaskObserver composite(List<? extends TaskObserver> observers) {
        List<TaskObserver> all = observers.stream().filter(observer -> observer != NONE).<TaskObserver>map(observer -> observer).toList();
        return all.isEmpty() ? NONE : all.size() == 1 ? all.getFirst() : new CompositeObserver(all);
    }

    /**
     * Carries these MDC keys from the code that pushes a task to the code that handles it, for a
     * correlation id that follows the work through every topic, with or without tracing. Each
     * key the MDC has when a task is pushed is stored as a header of the same name, and is put
     * back in the MDC while a handler works on the task. Downstream work, pushed from there,
     * picks it up again.
     */
    static TaskObserver mdc(Collection<String> keys) {
        return new MdcHeaders(keys);
    }

    /**
     * Tasks are about to be written to a topic, inside the pushing transaction, on the pushing
     * thread: for downstream work of a batch, with the task that staged it opened
     * ({@link BatchObservation#task}). The returned observation's
     * {@link PushObservation#headers()} are stored with each task, and it is closed once the write
     * is done, whether the transaction commits or not.
     */
    default PushObservation pushing(TopicRef<?> topic, int tasks) {
        return PushObservation.NONE;
    }

    /** A push has committed, with one result per creation. */
    default void pushed(TopicRef<?> topic, List<Pushed> results) {
    }

    /**
     * A claimed batch is about to run. The engine opens the observation on the thread running the
     * handler and again on the thread completing the batch, and closes it once the batch is done.
     * Each task of the batch can have its own observation as well ({@link BatchObservation#task}).
     */
    default BatchObservation batch(TopicRef<?> topic, List<Task<?>> tasks) {
        return BatchObservation.NONE;
    }

    /** What became of a task when its batch completed. */
    enum Result {
        SUCCEEDED, FILTERED, SUSPENDED, DELAYED,
        /** Failed without using up an attempt: a failure type the retry policy doesn't count. */
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
            public void close() {
            }
        };

        /**
         * Headers to store with each task of the push, such as the current trace's propagation
         * headers. A creation's own headers win over these.
         */
        default Map<String, String> headers() {
            return Map.of();
        }

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
         * One task of the batch, for an observer that follows each task on its own, such as a
         * tracer continuing the trace each task was pushed from. The engine asks for it once per
         * task, at most: when {@link Batch#decide} or
         * {@code decideConcurrently} is about to run the task's decider, or else when the
         * completion writes the task's downstream work. It opens the observation around the
         * decider, on the decider's thread, and closes it when the decider returns; then opens it
         * again, inside the batch's {@link #open()}, while the completion writes the task's
         * downstream work, so that work is pushed from the task. Every task observation is closed
         * before the batch's.
         *
         * <p>By default the task has no observation of its own and is seen as part of the batch.
         */
        default TaskObservation task(Task<?> task) {
            BatchObservation batch = this;
            return new TaskObservation() {
                @Override
                public Scope open() {
                    return batch.open();
                }
            };
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

    /** One task of a batch: see {@link BatchObservation#task}. */
    interface TaskObservation {

        TaskObservation NONE = new TaskObservation() {
        };

        /**
         * Makes the task current on this thread until the scope is closed. Called again after
         * {@link #close()}, for the task's downstream work.
         */
        default Scope open() {
            return Scope.NONE;
        }

        /** The task's decider threw. Called at most once, before {@link #close()}. */
        default void error(Throwable error) {
        }

        /** The engine is done handling the task. Called exactly once. */
        default void close() {
        }
    }
}
