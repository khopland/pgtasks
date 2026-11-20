package org.example.task2;

import java.util.List;

/** A claimed batch of tasks from one topic. */
public interface Batch<T> {

    TopicRef<T> topic();

    List<Task<T>> tasks();

    default List<T> payloads() {
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

    @FunctionalInterface
    interface Decider<T> {
        Decision decide(Work<T> task) throws Exception;
    }
}
