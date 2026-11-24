package io.github.khopland.pgtasks.ratelimit.postgres;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;

import io.github.khopland.pgtasks.postgres.ExtensionSchema;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.ratelimit.SlidingCounter;
import io.github.khopland.pgtasks.ratelimit.SlidingWindowStore;
import io.github.khopland.pgtasks.spi.StoreTransaction;

/**
 * Sliding-window counters in the pgtasks schema, one row per topic. Each update takes the row's
 * lock in its own short transaction, so nodes take turns spending the same allowance; nothing is
 * held between claims. Created by {@link PostgresSlidingWindowCapability}, not directly by
 * applications. Its own table comes from {@link #SCHEMA}, not from the engine's schema files.
 */
public final class PostgresSlidingWindowStore implements SlidingWindowStore {

    /** This extension's SQL, shipped in its jar rather than added to the engine's V001. */
    public static final ExtensionSchema SCHEMA = ExtensionSchema.of("rate_limit", PostgresSlidingWindowStore.class,
        "io/github/khopland/pgtasks/ratelimit/postgres/db",
        List.of("V001__rate_limit_window.sql"), List.of("rate_limit_window"));

    private final PostgresTaskStore store;
    private final String table;
    private volatile boolean schemaReady;

    /** Created by the capability for a particular store. */
    PostgresSlidingWindowStore(PostgresTaskStore store) {
        this.store = store;
        table = store.schema() + ".rate_limit_window";
    }

    @Override
    public <R> R update(String topic, Function<SlidingCounter, R> action) {
        try {
            ensureSchema();
        } catch (SQLException | RuntimeException e) {
            throw new IllegalStateException("Could not apply " + SCHEMA.describe() + " to " + table
                + ": the cluster rate limit needs its own table", e);
        }
        try (StoreTransaction transaction = store.begin()) {
            ensureRow(transaction, topic);
            SlidingCounter counter = load(transaction, topic);
            R result = action.apply(counter);
            save(transaction, topic, counter);
            transaction.commit();
            return result;
        } catch (SQLException e) {
            throw new IllegalStateException("Could not update the shared rate limit for topic '" + topic + "' in " + table, e);
        }
    }

    /** Applies the extension's files once per store instance, according to the store's schema mode. */
    private synchronized void ensureSchema() throws SQLException {
        if (!schemaReady) {
            SCHEMA.apply(store);
            schemaReady = true;
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
