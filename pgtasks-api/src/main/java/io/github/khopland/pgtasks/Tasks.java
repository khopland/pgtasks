package io.github.khopland.pgtasks;

import java.sql.Connection;

/**
 * Transaction-aware enqueue. Choose explicitly whether the push joins a transaction:
 *
 * <pre>{@code
 * tasks.push(ASSESSMENT, id, request);                 // joins the current transaction, throws if there is none
 * tasks.on(connection).push(ASSESSMENT, id, request);  // joins the transaction on this connection
 * tasks.independently().push(ASSESSMENT, id, request); // commits on its own; the only call that does
 * }</pre>
 *
 * Workers are woken once the transaction commits. A rollback removes the tasks and the wake-up.
 */
public interface Tasks extends TaskWriter {

    /**
     * A writer on an explicit JDBC connection, for example a feed that advances its pointer in
     * the same commit. The connection must not be in auto-commit mode.
     *
     * <p>The engine doesn't see this transaction commit, so the push wakes workers from inside it,
     * as a {@code NOTIFY} at its commit. Postgres serializes the commits of every transaction that
     * notifies, so with many such transactions at once, prefer {@link #push} in a bound
     * transaction: the engine then notifies after the commit, outside it.
     */
    TaskWriter on(Connection connection);

    /** A writer that opens, commits and closes its own transaction for every push. */
    TaskWriter independently();
}
