package org.example.task2.spi;

import java.sql.Connection;
import java.util.Optional;

/**
 * Finds the transaction the calling code is running in, so {@code Tasks.push} can join it.
 * A Spring integration resolves the connection bound to the current Spring transaction.
 */
@FunctionalInterface
public interface TransactionBinding {

    /** The connection of the current transaction, if there is one. */
    Optional<Connection> current();

    static TransactionBinding none() {
        return Optional::empty;
    }
}
