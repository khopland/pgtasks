package org.example.task2.spi;

import java.sql.Connection;
import java.util.Optional;

/** The application transaction that {@code Tasks.push} runs in. */
public interface AppTransaction {

    /** The transaction's JDBC connection, if it has one. A Postgres store pushes on it. */
    Optional<Connection> connection();

    /**
     * Runs the action once the transaction has committed; never if it rolls back. A store that
     * can't join the transaction, such as the in-memory store, applies pushes this way.
     */
    void afterCommit(Runnable action);
}
