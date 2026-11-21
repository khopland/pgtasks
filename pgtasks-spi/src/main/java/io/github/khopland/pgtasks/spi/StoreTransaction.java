package io.github.khopland.pgtasks.spi;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A transaction in a task store. Every store operation runs in one. Closing a transaction that
 * was neither committed nor rolled back rolls it back; closing a joined transaction leaves it to
 * its owner.
 */
public interface StoreTransaction extends AutoCloseable {

    void commit() throws SQLException;

    void rollback() throws SQLException;

    /**
     * The JDBC connection of this transaction, for batch resources' JDBC effects. A store without
     * one, such as the in-memory store, returns a connection that fails with a clear message
     * when it is used.
     */
    Connection connection();

    @Override
    void close() throws SQLException;
}
