package org.example.task2.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.example.task2.spi.TransactionBinding;

/**
 * Plain-JDBC transactions for code that doesn't use a framework: runs a callback in a
 * transaction and binds its connection, so {@code Tasks.push} joins it.
 *
 * <pre>{@code
 * transactions.inTransaction(connection -> {
 *     returns.save(connection, taxReturn);
 *     tasks.push(ASSESSMENT, taxReturn.id(), request);  // same commit
 *     return null;
 * });
 * }</pre>
 */
public final class JdbcTransactions implements TransactionBinding {

    private static final ScopedValue<Connection> CURRENT = ScopedValue.newInstance();

    private final DataSource dataSource;

    public JdbcTransactions(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource);
    }

    @FunctionalInterface
    public interface Callback<T> {
        T run(Connection connection) throws Exception;
    }

    /** Commits when the callback returns and rolls back when it throws. Does not nest. */
    public <T> T inTransaction(Callback<T> callback) throws Exception {
        if (CURRENT.isBound()) {
            throw new IllegalStateException("Already in a transaction");
        }
        try (Connection connection = dataSource.getConnection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                T result = ScopedValue.where(CURRENT, connection).call(() -> callback.run(connection));
                connection.commit();
                return result;
            } catch (Exception | Error e) {
                try {
                    connection.rollback();
                } catch (SQLException rollback) {
                    e.addSuppressed(rollback);
                }
                throw e;
            } finally {
                connection.setAutoCommit(autoCommit);
            }
        }
    }

    @Override
    public Optional<Connection> current() {
        return CURRENT.isBound() ? Optional.of(CURRENT.get()) : Optional.empty();
    }
}
