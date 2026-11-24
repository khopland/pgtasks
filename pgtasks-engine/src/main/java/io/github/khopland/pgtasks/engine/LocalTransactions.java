package io.github.khopland.pgtasks.engine;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.github.khopland.pgtasks.spi.AppTransaction;
import io.github.khopland.pgtasks.spi.TransactionBinding;

/**
 * A transaction scope without a database, for apps and tests on the in-memory store outside
 * Spring: pushes made inside {@link #inTransaction} are held back and applied when the callback
 * returns, and dropped when it throws.
 */
public final class LocalTransactions implements TransactionBinding {

    /** Creates a binding; share one instance with the engine and the code that uses it. */
    public LocalTransactions() {
    }

    private static final ScopedValue<Scope> CURRENT = ScopedValue.newInstance();

    @FunctionalInterface
    public interface Callback<T> {
        T run() throws Exception;
    }

    public <T> T inTransaction(Callback<T> callback) throws Exception {
        if (CURRENT.isBound()) {
            throw new IllegalStateException("Already in a transaction");
        }
        Scope scope = new Scope();
        T result = ScopedValue.where(CURRENT, scope).call(callback::run);
        try {
            AfterCommit.runAll(scope.afterCommit);
        } catch (RuntimeException e) {
            throw new IllegalStateException("The transaction committed, but an action after the commit failed", e);
        }
        return result;
    }

    @Override
    public Optional<AppTransaction> current() {
        return CURRENT.isBound() ? Optional.of(CURRENT.get()) : Optional.empty();
    }

    private static final class Scope implements AppTransaction {

        private final List<Runnable> afterCommit = new ArrayList<>();

        @Override
        public Optional<Connection> connection() {
            return Optional.empty();
        }

        @Override
        public void afterCommit(Runnable action) {
            afterCommit.add(action);
        }
    }
}
