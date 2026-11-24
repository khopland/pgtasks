package io.github.khopland.pgtasks.spring;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;

import io.github.khopland.pgtasks.spi.AppTransaction;
import io.github.khopland.pgtasks.spi.TransactionBinding;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Lets {@code tasks.push(...)} join the current Spring transaction: on Postgres it writes on the
 * transaction's own connection, so the tasks commit or roll back with the application's data; on
 * the in-memory store the pushes are held back until the transaction commits.
 *
 * <p>Only a transaction on the tasks' DataSource has a connection to join. Inside a transaction
 * on another DataSource a push to Postgres fails instead of writing on a connection of its own,
 * which would commit whatever the application's transaction does.
 */
public final class SpringTransactions implements TransactionBinding {

    private final @Nullable DataSource dataSource;

    /** @param dataSource the tasks' DataSource; null for a store outside the database, such as the in-memory store */
    public SpringTransactions(@Nullable DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Optional<AppTransaction> current() {
        if (!TransactionSynchronizationManager.isSynchronizationActive() || !TransactionSynchronizationManager.isActualTransactionActive()) {
            return Optional.empty();
        }
        return Optional.of(new AppTransaction() {
            @Override
            public Optional<Connection> connection() {
                if (dataSource == null || !TransactionSynchronizationManager.hasResource(dataSource)) {
                    return Optional.empty();
                }
                // The transaction's connection; Spring closes it when the transaction ends.
                return Optional.of(DataSourceUtils.getConnection(dataSource));
            }

            @Override
            public void afterCommit(Runnable action) {
                // One synchronization per transaction runs them all: Spring stops at the first
                // synchronization that throws, which would drop every later push of the transaction.
                // A suspended transaction's synchronizations aren't listed, so an inner REQUIRES_NEW
                // transaction gets its own.
                for (TransactionSynchronization synchronization : TransactionSynchronizationManager.getSynchronizations()) {
                    if (synchronization instanceof AfterCommitActions actions && actions.owner == SpringTransactions.this) {
                        actions.actions.add(action);
                        return;
                    }
                }
                AfterCommitActions actions = new AfterCommitActions(SpringTransactions.this);
                actions.actions.add(action);
                TransactionSynchronizationManager.registerSynchronization(actions);
            }
        });
    }

    /** The after-commit actions of one transaction; each runs even if one before it fails. */
    private static final class AfterCommitActions implements TransactionSynchronization {

        private final SpringTransactions owner;
        private final List<Runnable> actions = new ArrayList<>();

        AfterCommitActions(SpringTransactions owner) {
            this.owner = owner;
        }

        @Override
        public void afterCommit() {
            RuntimeException failure = null;
            for (Runnable action : actions) {
                try {
                    action.run();
                } catch (RuntimeException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
