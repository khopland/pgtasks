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
 * before it. That holds in read committed transactions, the Postgres default.
 *
 * <p>A transaction holds these waits until it commits, and only the identifiers of one call are
 * taken in a fixed order. If one transaction pushes {@code a} and then {@code b} in separate calls
 * while another pushes {@code b} and then {@code a}, each waits for the other, and Postgres aborts
 * one of them with a deadlock (SQLState {@code 40P01}). The engine retries its own completions on
 * that, but an application transaction gets the exception. So push several identifiers in one
 * call, or in the same order in every producer, and retry the transaction on a deadlock.
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
