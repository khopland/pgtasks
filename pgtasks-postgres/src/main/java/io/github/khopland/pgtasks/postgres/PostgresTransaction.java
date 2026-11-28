package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;

import io.github.khopland.pgtasks.spi.StoreTransaction;

import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A store transaction on a JDBC connection: either its own, or an application's it joined.
 *
 * <p>The store's own transactions run at READ COMMITTED, whatever the pool or the database
 * defaults to. A SINGULAR_* claim takes the topic's advisory lock and then polls in a separate
 * statement, which must see what the claim that held the lock before it committed; under
 * REPEATABLE READ its snapshot is older than the lock, so two nodes could claim one identifier.
 * Claims and completions would also fail with serialization errors.
 *
 * <p>A joined transaction is the application's, and its isolation is the application's choice; the
 * store assumes READ COMMITTED there too. A push joined to a REPEATABLE READ or SERIALIZABLE
 * transaction works, but a SUPERSEDE or REPLACE push in one may not see an older task for its
 * identifier that was pushed concurrently.
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
    /** The network timeout to restore on close, or -1 when it wasn't changed. */
    private final int restoreNetworkTimeout;
    private boolean done;
    /** Undos of state outside the database, newest first; see {@link #onRollback}. */
    private final Deque<Runnable> undo = new ArrayDeque<>();

    private PostgresTransaction(Connection connection, boolean owned, int restore, int restoreNetworkTimeout) {
        this.connection = connection;
        this.owned = owned;
        this.restore = restore;
        this.restoreNetworkTimeout = restoreNetworkTimeout;
    }

    /**
     * @param networkTimeout how long each statement waits for the database's reply before the
     *                       connection is given up as broken; see {@link PostgresTaskStore#NETWORK_TIMEOUT}.
     *                       A local setting of the socket, which costs no round trip
     */
    static PostgresTransaction open(Connection connection, Duration networkTimeout) throws SQLException {
        int restore = -1;
        int restoreNetworkTimeout = -1;
        try {
            int previous = connection.getNetworkTimeout();
            int bound = Math.toIntExact(Math.min(networkTimeout.toMillis(), Integer.MAX_VALUE));
            if (previous == 0 || previous > bound) {
                connection.setNetworkTimeout(Runnable::run, bound);
                restoreNetworkTimeout = previous;
            }
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
                if (restoreNetworkTimeout != -1) {
                    connection.setNetworkTimeout(Runnable::run, restoreNetworkTimeout);
                }
            } catch (SQLException | RuntimeException suppressed) {
                e.addSuppressed(suppressed);
            } finally {
                connection.close();
            }
            throw e;
        }
        return new PostgresTransaction(connection, true, restore, restoreNetworkTimeout);
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
        return new PostgresTransaction(connection, false, -1, -1);
    }

    /** Whether the store began this transaction, rather than joined an application's. */
    boolean owned() {
        return owned;
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
        undo.clear();
    }

    @Override
    public void rollback() throws SQLException {
        try {
            if (owned) {
                connection.rollback();
            }
        } finally {
            done = true;
            undoAll(0);
        }
    }

    @Override
    public void onRollback(Runnable action) {
        Objects.requireNonNull(action, "action");
        if (!owned) {
            throw new UnsupportedOperationException("A joined transaction rolls back with its owner, out of the store's sight");
        }
        undo.push(action);
    }

    /** Undoes, with JDBC's savepoint, the undos recorded since too. */
    @Override
    public Savepoint savepoint() throws SQLException {
        java.sql.Savepoint savepoint = connection.setSavepoint();
        int changes = undo.size();
        return new Savepoint() {
            @Override
            public void rollback() throws SQLException {
                try {
                    connection.rollback(savepoint);
                    connection.releaseSavepoint(savepoint);
                } finally {
                    undoAll(changes);
                }
            }

            @Override
            public void release() throws SQLException {
                connection.releaseSavepoint(savepoint);
            }
        };
    }

    /** Runs the undos recorded after the first {@code keep}, newest first; one that throws is logged. */
    private void undoAll(int keep) {
        while (undo.size() > keep) {
            try {
                undo.pop().run();
            } catch (RuntimeException e) {
                LOGGER.warn("An undo on rollback failed", e);
            }
        }
    }

    @Override
    public void close() throws SQLException {
        if (!owned) {
            return;
        }
        try {
            if (!done) {
                try {
                    connection.rollback();
                } finally {
                    undoAll(0);
                }
            }
            if (restore != -1) {
                // Not every pool resets the level when the connection goes back, and the cache
                // above assumes it is what it was.
                connection.setTransactionIsolation(restore);
            }
            if (restoreNetworkTimeout != -1) {
                // The application's own use of the connection keeps its own timeout.
                connection.setNetworkTimeout(Runnable::run, restoreNetworkTimeout);
            }
        } finally {
            connection.close();
        }
    }
}
