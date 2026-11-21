package io.github.khopland.task2;

/**
 * What a push does to unfinished and finished tasks with the same identifier in the same topic.
 * Tasks that a worker has already claimed are never touched, and rows locked by a concurrent
 * transaction are skipped, so these are queue-management tools, not a uniqueness guarantee. Use
 * a {@link Creation#dedupKey() dedup key} for that.
 */
public enum Insertion {

    /** Adds the task and leaves every other task alone. */
    APPEND,

    /**
     * Earlier tasks that are still waiting to run (ready, delayed or backing off) become
     * redundant. Within one push, only the last creation per identifier is queued.
     */
    SUPERSEDE,

    /**
     * Like {@link #SUPERSEDE}, but also covers suspended and dead-lettered tasks, and marks
     * finished tasks (succeeded, filtered, failed) redundant, keeping their outcome in
     * {@code previous_outcome}.
     */
    REPLACE,

    /**
     * Deletes every earlier task for the identifier that isn't running, history included. Within
     * one push, only the last creation per identifier is kept.
     */
    DELETE
}
