package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

import io.github.khopland.pgtasks.spi.StoreTransaction;

import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A store transaction on a JDBC connection: either its own, or an application's it joined.
 *
 * <p>The store's own transactions run at READ COMMITTED, whatever the pool or the database
 * defaults to. The fan-in check takes advisory locks and then counts in a separate statement, which
 * must see what the parts that held those locks before it committed; under REPEATABLE READ its
 * snapshot is older than the lock, two parts completing at once each miss the other, and the
 * group stays suspended for good. Claims would also fail with serialization errors.
 *
 * <p>A joined transaction is the application's, and its isolation is the application's choice; the
 * store assumes READ COMMITTED there too. A push joined to a REPEATABLE READ or SERIALIZABLE
 * transaction works, but a fan-in check in one can miss a part committed concurrently.
 */
final class PostgresTransaction implements StoreTransaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresTransaction.class);

    /**
     * The isolation each physical connection runs at when the store gets it, so the common case,
     * a connection already at READ COMMITTED, costs one query per connection instead of one per
     * transaction. A connection the store moves to READ COMMITTED goes back to what it was before
     * it is closed, so the cached level stays true. Weak keys: physical connections come and go.
     */
    private static final Map<Object, Integer> NATIVE_ISOLATION = Collections.synchronizedMap(new WeakHashMap<>());
    private static volatile boolean warned;

    private final Connection connection;
    private final boolean owned;
    /** The isolation to restore on close, or -1 when the connection was already at READ COMMITTED. */
    private final int restore;
    private boolean done;

    private PostgresTransaction(Connection connection, boolean owned, int restore) {
        this.connection = connection;
        this.owned = owned;
        this.restore = restore;
    }

    static PostgresTransaction open(Connection connection) throws SQLException {
        int restore = -1;
        try {
            int isolation = nativeIsolation(connection);
            if (isolation != Connection.TRANSACTION_READ_COMMITTED) {
                if (!warned) {
                    warned = true;
                    LOGGER.warn("The pool's connections default to {}; the task store runs its own transactions at READ COMMITTED, "
                        + "which costs two extra round trips per transaction. Configure the pool or database for READ COMMITTED "
                        + "to avoid them", name(isolation));
                }
                // Before setAutoCommit(false): the level can only change between transactions.
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                restore = isolation;
            }
            connection.setAutoCommit(false);
        } catch (SQLException | RuntimeException e) {
            try {
                if (restore != -1) {
                    connection.setTransactionIsolation(restore);
                }
            } catch (SQLException | RuntimeException suppressed) {
                e.addSuppressed(suppressed);
            } finally {
                connection.close();
            }
            throw e;
        }
        return new PostgresTransaction(connection, true, restore);
    }

    private static int nativeIsolation(Connection connection) throws SQLException {
        Object physical;
        try {
            physical = connection.unwrap(PGConnection.class);
        } catch (SQLException e) {
            physical = connection;
        }
        Integer known = NATIVE_ISOLATION.get(physical);
        if (known != null) {
            return known;
        }
        int isolation = connection.getTransactionIsolation();
        NATIVE_ISOLATION.put(physical, isolation);
        return isolation;
    }

    private static String name(int isolation) {
        return switch (isolation) {
            case Connection.TRANSACTION_READ_UNCOMMITTED -> "READ UNCOMMITTED";
            case Connection.TRANSACTION_READ_COMMITTED -> "READ COMMITTED";
            case Connection.TRANSACTION_REPEATABLE_READ -> "REPEATABLE READ";
            case Connection.TRANSACTION_SERIALIZABLE -> "SERIALIZABLE";
            default -> "isolation " + isolation;
        };
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
        return new PostgresTransaction(connection, false, -1);
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
            if (restore != -1) {
                // Not every pool resets the level when the connection goes back, and the cache
                // above assumes it is what it was.
                connection.setTransactionIsolation(restore);
            }
        } finally {
            connection.close();
        }
    }
}
