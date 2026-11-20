package org.example.task2.postgres;

import java.sql.Connection;
import java.sql.SQLException;

import org.example.task2.spi.StoreTransaction;

/** A store transaction on a JDBC connection: either its own, or an application's it joined. */
final class PostgresTransaction implements StoreTransaction {

    private final Connection connection;
    private final boolean owned;
    private boolean done;

    private PostgresTransaction(Connection connection, boolean owned) {
        this.connection = connection;
        this.owned = owned;
    }

    static PostgresTransaction open(Connection connection) throws SQLException {
        try {
            connection.setAutoCommit(false);
        } catch (SQLException | RuntimeException e) {
            connection.close();
            throw e;
        }
        return new PostgresTransaction(connection, true);
    }

    static PostgresTransaction join(Connection connection) {
        try {
            if (connection.getAutoCommit()) {
                throw new IllegalStateException("The connection is in auto-commit mode, so the push would commit on its own. "
                    + "Start a transaction first, or use tasks.independently() if that is what you want.");
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not inspect the connection", e);
        }
        return new PostgresTransaction(connection, false);
    }

    @Override
    public Connection connection() {
        return connection;
    }

    @Override
    public void commit() throws SQLException {
        if (owned) {
            connection.commit();
        }
        done = true;
    }

    @Override
    public void rollback() throws SQLException {
        if (owned) {
            connection.rollback();
        }
        done = true;
    }

    @Override
    public void close() throws SQLException {
        if (!owned) {
            return;
        }
        try {
            if (!done) {
                connection.rollback();
            }
        } finally {
            connection.close();
        }
    }
}
