package io.github.khopland.pgtasks.ratelimit.postgres;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.ratelimit.SlidingCounter;
import io.github.khopland.pgtasks.ratelimit.SlidingWindowStore;
import io.github.khopland.pgtasks.spi.StoreTransaction;

/**
 * Sliding-window counters in the pgtasks schema, one row per topic. Each update takes the row's
 * lock in its own short transaction, so nodes take turns spending the same allowance; nothing is
 * held between claims. Created by {@link PostgresSlidingWindowCapability}, not directly by
 * applications.
 */
public final class PostgresSlidingWindowStore implements SlidingWindowStore {

    private final PostgresTaskStore store;
    private final String table;

    /** Created by the capability for a particular store. */
    PostgresSlidingWindowStore(PostgresTaskStore store) {
        this.store = store;
        String schema = store.schema();
        table = schema + ".rate_limit_window";
    }

    @Override
    public <R> R update(String topic, Function<SlidingCounter, R> action) {
        try (StoreTransaction transaction = store.begin()) {
            ensureRow(transaction, topic);
            SlidingCounter counter = load(transaction, topic);
            R result = action.apply(counter);
            save(transaction, topic, counter);
            transaction.commit();
            return result;
        } catch (SQLException e) {
            throw new IllegalStateException("Could not update the shared rate limit for topic '" + topic
                + "': is the pgtasks schema migrated so it has " + table + "?", e);
        }
    }

    private void ensureRow(StoreTransaction transaction, String topic) throws SQLException {
        String sql = "INSERT INTO " + table + " (topic, window_start) VALUES (?, ?) ON CONFLICT (topic) DO NOTHING";
        try (PreparedStatement statement = transaction.connection().prepareStatement(sql)) {
            statement.setString(1, topic);
            statement.setLong(2, store.clock().instant().toEpochMilli());
            statement.executeUpdate();
        }
    }

    private SlidingCounter load(StoreTransaction transaction, String topic) throws SQLException {
        String sql = "SELECT window_start, previous, current FROM " + table + " WHERE topic = ? FOR UPDATE";
        try (PreparedStatement statement = transaction.connection().prepareStatement(sql)) {
            statement.setString(1, topic);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new SQLException("No counter row for " + topic + " after inserting it");
                }
                return new SlidingCounter(resultSet.getLong(1), resultSet.getLong(2), resultSet.getLong(3));
            }
        }
    }

    private void save(StoreTransaction transaction, String topic, SlidingCounter counter) throws SQLException {
        String sql = "UPDATE " + table + " SET window_start = ?, previous = ?, current = ? WHERE topic = ?";
        try (PreparedStatement statement = transaction.connection().prepareStatement(sql)) {
            statement.setLong(1, counter.windowStart());
            statement.setLong(2, counter.previous());
            statement.setLong(3, counter.current());
            statement.setString(4, topic);
            statement.executeUpdate();
        }
    }
}
