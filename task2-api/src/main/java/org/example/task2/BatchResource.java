package org.example.task2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Something a topic opens once per batch: lookups, staging buffers, clients.
 *
 * <p>Completion has two phases. First every resource {@linkplain #review reviews} the handler's
 * decisions and may revise outcomes; the engine merges what they return, the most severe
 * decision winning. Only then does anything get written, and every resource sees the same
 * final decisions: JDBC effects in {@link #beforeCommit} on the completion connection, external
 * effects in {@link #afterCommit}. So a resource never writes work for a task that another
 * resource stopped.
 *
 * <p>If the opened value is {@link AutoCloseable}, the engine closes it when the batch is over,
 * whatever the outcome.
 *
 * @param <R> what the resource opens per batch
 */
public interface BatchResource<R> {

    R open(TopicRef<?> topic, List<Task<?>> tasks) throws Exception;

    /** Phase 1: may revise outcomes. Must not write anything. */
    default Decisions review(R resource, Decisions decisions) throws Exception {
        return decisions;
    }

    /**
     * Phase 2: JDBC effects on the completion connection. They commit or roll back together
     * with the completion, including when a stale claim makes the completion roll back.
     */
    default void beforeCommit(R resource, Decisions finalDecisions, Connection connection) throws SQLException {
    }

    /**
     * After the completion committed: external effects and wake-ups. These can't be rolled
     * back and run again if a worker dies before this point, so deduplicate them with
     * {@link Task#idempotencyKey()}.
     */
    default void afterCommit(R resource, Decisions finalDecisions) {
    }
}
