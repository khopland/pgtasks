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

    /**
     * Marks a point to roll back to without ending the transaction, so one failing step of many
     * can be undone and the rest kept. The default uses a JDBC savepoint on {@link #connection()};
     * a store without a connection overrides it.
     */
    default Savepoint savepoint() throws SQLException {
        Connection connection = connection();
        java.sql.Savepoint savepoint = connection.setSavepoint();
        return new Savepoint() {
            @Override
            public void rollback() throws SQLException {
                connection.rollback(savepoint);
                connection.releaseSavepoint(savepoint);
            }

            @Override
            public void release() throws SQLException {
                connection.releaseSavepoint(savepoint);
            }
        };
    }

    /** A point in a transaction, from {@link #savepoint()}. Ends with exactly one of its methods. */
    interface Savepoint {

        /** Undoes everything done in the transaction since the savepoint; the transaction goes on. */
        void rollback() throws SQLException;

        /** Keeps what was done since the savepoint, as part of the transaction. */
        void release() throws SQLException;
    }

    @Override
    void close() throws SQLException;
}
