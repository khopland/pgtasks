package io.github.khopland.pgtasks;

/**
 * When a later task for the same identifier, in the same topic, waits for an earlier one.
 *
 * <table>
 *   <caption>The earlier task is...</caption>
 *   <tr><th></th><th>SINGULAR</th><th>..._ON_FAILURE</th><th>..._UNTIL_SUCCESS</th></tr>
 *   <tr><td>ready or active</td><td>waits</td><td>waits</td><td>waits</td></tr>
 *   <tr><td>backing off before a retry</td><td>waits</td><td>waits</td><td>waits</td></tr>
 *   <tr><td>expired (requeued)</td><td>waits</td><td>waits</td><td>waits</td></tr>
 *   <tr><td>suspended or delayed</td><td>runs</td><td>runs</td><td>waits</td></tr>
 *   <tr><td>dead-lettered</td><td>runs</td><td>waits</td><td>waits</td></tr>
 *   <tr><td>finished</td><td>runs</td><td>runs</td><td>runs</td></tr>
 * </table>
 *
 * <p>Under every {@code SINGULAR_*} ordering, an active task also holds back every other task for
 * its identifier, earlier or later, so two tasks for one identifier never run at the same time.
 *
 * <p>Earlier and later mean task-id order. A push draws its ids before it commits, so concurrent
 * pushes for one identifier can commit out of id order. Pushes with any {@link Insertion} other
 * than {@link Insertion#APPEND APPEND} wait for each other per identifier until commit, so their
 * ids follow commit order. Concurrent {@code APPEND} pushes for one identifier don't, and their
 * tasks may become visible out of id order: a later task can start first, and the earlier one
 * then waits until it is done. Push from one transaction at a time per identifier, or use another
 * insertion, when the order between concurrent producers matters.
 */
public enum Ordering {

    /** By priority, then by sequence. No per-identifier constraint. The cheapest option. */
    FIFO,

    SINGULAR_BY_IDENTIFIER,

    SINGULAR_BY_IDENTIFIER_ON_FAILURE,

    SINGULAR_BY_IDENTIFIER_UNTIL_SUCCESS
}
