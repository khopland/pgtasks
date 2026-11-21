package io.github.khopland.pgtasks;

/**
 * When a later task for the same identifier, in the same topic, waits for an earlier one.
 *
 * <table>
 *   <caption>The earlier task is...</caption>
 *   <tr><th></th><th>SINGULAR</th><th>..._ON_FAILURE</th><th>..._UNTIL_SUCCESS</th></tr>
 *   <tr><td>ready or active</td><td>waits</td><td>waits</td><td>waits</td></tr>
 *   <tr><td>backing off before a retry, or throttled</td><td>waits</td><td>waits</td><td>waits</td></tr>
 *   <tr><td>expired (requeued)</td><td>waits</td><td>waits</td><td>waits</td></tr>
 *   <tr><td>suspended or delayed</td><td>runs</td><td>runs</td><td>waits</td></tr>
 *   <tr><td>dead-lettered</td><td>runs</td><td>waits</td><td>waits</td></tr>
 *   <tr><td>finished</td><td>runs</td><td>runs</td><td>runs</td></tr>
 * </table>
 *
 * <p>Under every {@code SINGULAR_*} ordering, an active task also holds back every other task for
 * its identifier, earlier or later, so two tasks for one identifier never run at the same time.
 */
public enum Ordering {

    /** By priority, then by sequence. No per-identifier constraint. The cheapest option. */
    FIFO,

    SINGULAR_BY_IDENTIFIER,

    SINGULAR_BY_IDENTIFIER_ON_FAILURE,

    SINGULAR_BY_IDENTIFIER_UNTIL_SUCCESS
}
