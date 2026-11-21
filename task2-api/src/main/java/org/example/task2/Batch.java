package org.example.task2;

import java.util.List;

import org.jspecify.annotations.Nullable;

/** A claimed batch of tasks from one topic. */
public interface Batch<T> {

    TopicRef<T> topic();

    List<Task<T>> tasks();

    default List<@Nullable T> payloads() {
        return tasks().stream().map(Task::payload).toList();
    }

    default List<String> identifiers() {
        return tasks().stream().map(Task::identifier).toList();
    }

    /** The value a batch resource opened for this batch. */
    <R> R resource(BatchResource<R> resource);

    /**
     * Decides each task in turn. An exception from the decider fails only that task.
     */
    Decisions decide(Decider<T> decider);

    /**
     * Decides up to {@code parallelism} tasks at once, each on a virtual thread of its own, for a
     * decider that waits on something, such as a remote call per task: with {@link #decide} a
     * batch of 100 makes its calls one after another. An exception fails only its task, and the
     * decisions come back in the batch's order. Staged work is per task, as with {@link #decide};
     * anything else the decider shares across tasks must be thread-safe.
     */
    Decisions decideConcurrently(int parallelism, Decider<T> decider);

    @FunctionalInterface
    interface Decider<T> {
        Decision decide(Work<T> task) throws Exception;
    }
}
