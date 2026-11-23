package io.github.khopland.pgtasks;

/**
 * What a push does to unfinished and finished tasks with the same identifier in the same topic.
 * Tasks that a worker has already claimed are never touched, and rows locked by a concurrent
 * transaction are skipped, so these are queue-management tools, not a uniqueness guarantee. Use
 * a {@link Creation#dedupKey() dedup key} for that.
 *
 * <p>"Earlier" means a lower task id. Every insertion but {@link #APPEND} waits for other
 * uncommitted pushes of its identifiers (other than {@code APPEND} pushes) to commit or roll
 * back before it draws its ids, so a push always finds the tasks of pushes that took their ids
 * before it. That holds in read committed transactions, the Postgres default. A transaction that
 * pushes for several identifiers holds them until it commits, so push them in one call, or in
 * the same order everywhere, to avoid deadlocks between producers.
 */
public enum Insertion {

    /**
     * Adds the task and leaves every other task alone. It waits for no other push, so concurrent
     * {@code APPEND} pushes for one identifier may commit out of id order (see {@link Ordering}).
     */
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
