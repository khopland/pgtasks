package org.example.task2.slice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

/**
 * The retry-safe change the assessment topic owns. It commits on its own, outside the task's
 * completion, like a call to another system would, so it deduplicates by the task's
 * idempotency key: a retry after a crash finds the change already applied.
 */
public final class AssessmentLedger {

    private final DataSource dataSource;
    public final AtomicInteger calls = new AtomicInteger();

    public AssessmentLedger(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** Returns whether this call applied the change; false if it was already applied. */
    public boolean apply(String idempotencyKey, String person, long tax) throws SQLException {
        calls.incrementAndGet();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                 "INSERT INTO assessment (idempotency_key, person, tax) VALUES (?, ?, ?) ON CONFLICT (idempotency_key) DO NOTHING")) {
            statement.setString(1, idempotencyKey);
            statement.setString(2, person);
            statement.setLong(3, tax);
            return statement.executeUpdate() == 1;
        }
    }
}
