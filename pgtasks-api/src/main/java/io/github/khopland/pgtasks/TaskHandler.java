package io.github.khopland.pgtasks;

/**
 * Handles one task at a time, for the common case: returning means the task succeeded, throwing
 * means it failed, and the topic's retry policy decides what happens next. Downstream work goes
 * through {@link Work#push}, committed with the task's completion. Register it with
 * {@code tasks.eachTask(TOPIC, task -> ...)}, or wrap it with {@link BatchHandler#each}.
 *
 * <p>For other decisions (filtered, later, suspended, throttled, unavailable), or to do work once
 * per batch, write a {@link BatchHandler} and use {@link Batch#decide}.
 */
@FunctionalInterface
public interface TaskHandler<T> {

    void handle(Work<T> task) throws Exception;
}
