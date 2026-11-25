package io.github.khopland.pgtasks.ratelimit.postgres;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import io.github.khopland.pgtasks.postgres.ExtensionSchema;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.ratelimit.SlidingCounter;
import io.github.khopland.pgtasks.ratelimit.SlidingWindowStore;
import io.github.khopland.pgtasks.spi.StoreTransaction;

import org.jspecify.annotations.Nullable;

/**
 * Sliding-window counters in the pgtasks schema, one row per topic. Each update locks the row in its
 * own short transaction and counts by the database's clock, so nodes take turns spending the same
 * allowance and agree on where the window is; nothing is held between claims. Created by {@link PostgresSlidingWindowCapability}, not directly by
 * applications. Its own table comes from {@link #SCHEMA}, not from the engine's schema files. Rows are keyed by
 * topic name and outlive a deleted topic; a counter that isn't touched for two of its windows no
 * longer holds anything back.
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
    public void prepare() {
        try {
            ensureSchema();
        } catch (SQLException | RuntimeException e) {
            throw new IllegalStateException("Could not apply " + SCHEMA.describe() + " to " + table
                + ": the cluster rate limit needs its own table", e);
        }
    }

    @Override
    public <R> R update(String topic, Update<R> action) {
        prepare();
        try (StoreTransaction transaction = store.begin()) {
            Loaded loaded = lock(transaction, topic);
            if (loaded == null) {
                insert(transaction, topic);
                loaded = lock(transaction, topic);
                if (loaded == null) {
                    throw new SQLException("No counter row for " + topic + " after inserting it");
                }
            }
            R result = action.apply(loaded.counter(), loaded.now());
            if (loaded.counter().changed()) {
                save(transaction, topic, loaded.counter());
            }
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

    private record Loaded(SlidingCounter counter, long now) {
    }

    /**
     * Locks the topic's row and reads it with the database's time, so every node counts by one
     * clock. Null when the topic has no row yet.
     */
    private @Nullable Loaded lock(StoreTransaction transaction, String topic) throws SQLException {
        // Guarded like the core store's claims: every node's claims on the topic wait for this row,
        // so a node that goes silent holding it must not hold it for longer than this.
        String sql = "SELECT window_start, previous, current, "
            + "(extract(epoch FROM clock_timestamp()) * 1000)::bigint, "
            + "set_config('idle_in_transaction_session_timeout', '" + PostgresTaskStore.STUCK_TRANSACTION_TIMEOUT.toMillis() + "', true) "
            + "FROM " + table + " WHERE topic = ? FOR UPDATE";
        try (PreparedStatement statement = transaction.connection().prepareStatement(sql)) {
            statement.setString(1, topic);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                return new Loaded(new SlidingCounter(resultSet.getLong(1), resultSet.getLong(2), resultSet.getLong(3)),
                    resultSet.getLong(4));
            }
        }
    }

    /** A new topic's row starts empty; its first admission moves it onto the current window. */
    private void insert(StoreTransaction transaction, String topic) throws SQLException {
        String sql = "INSERT INTO " + table + " (topic, window_start) VALUES (?, 0) ON CONFLICT (topic) DO NOTHING";
        try (PreparedStatement statement = transaction.connection().prepareStatement(sql)) {
            statement.setString(1, topic);
            statement.executeUpdate();
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
