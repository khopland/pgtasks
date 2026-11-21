package io.github.khopland.pgtasks.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;

import io.github.khopland.pgtasks.spi.AppTransaction;
import io.github.khopland.pgtasks.spi.TransactionBinding;

/**
 * Plain-JDBC transactions for code that doesn't use a framework: runs a callback in a
 * transaction and binds it, so {@code Tasks.push} joins it.
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

    private static final ScopedValue<Bound> CURRENT = ScopedValue.newInstance();

    private final DataSource dataSource;

    public JdbcTransactions(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource);
    }

    @FunctionalInterface
    public interface Callback<T> {
        T run(Connection connection) throws Exception;
    }

    /**
     * Commits when the callback returns and rolls back when it throws. Does not nest. If an
     * action registered to run after the commit fails, the transaction has still committed, and
     * the exception says so.
     */
    public <T> T inTransaction(Callback<T> callback) throws Exception {
        if (CURRENT.isBound()) {
            throw new IllegalStateException("Already in a transaction");
        }
        try (Connection connection = dataSource.getConnection()) {
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            Bound bound = new Bound(connection);
            T result;
            try {
                result = ScopedValue.where(CURRENT, bound).call(() -> callback.run(connection));
                connection.commit();
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
            try {
                bound.committed();
            } catch (RuntimeException e) {
                throw new IllegalStateException("The transaction committed, but an action after the commit failed", e);
            }
            return result;
        }
    }

    @Override
    public Optional<AppTransaction> current() {
        return CURRENT.isBound() ? Optional.of(CURRENT.get()) : Optional.empty();
    }

    private static final class Bound implements AppTransaction {

        private final Connection connection;
        private final List<Runnable> afterCommit = new ArrayList<>();

        Bound(Connection connection) {
            this.connection = connection;
        }

        @Override
        public Optional<Connection> connection() {
            return Optional.of(connection);
        }

        @Override
        public void afterCommit(Runnable action) {
            afterCommit.add(action);
        }

        void committed() {
            afterCommit.forEach(Runnable::run);
        }
    }
}
