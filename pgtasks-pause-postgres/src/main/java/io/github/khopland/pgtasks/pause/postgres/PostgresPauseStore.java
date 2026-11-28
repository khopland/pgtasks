package io.github.khopland.pgtasks.pause.postgres;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import io.github.khopland.pgtasks.pause.PauseStore;
import io.github.khopland.pgtasks.postgres.ExtensionSchema;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.spi.StoreTransaction;

/**
 * Paused topics in the pgtasks schema, one row per topic, in the transaction the extension opened
 * on the store. Created by {@link PostgresPauseCapability}, not directly by applications. Its own
 * table comes from {@link #SCHEMA}, not from the engine's schema files.
 */
public final class PostgresPauseStore implements PauseStore {

    /** This extension's SQL, shipped in its jar rather than added to the engine's V001. */
    public static final ExtensionSchema SCHEMA = ExtensionSchema.of("pause", PostgresPauseStore.class,
        "io/github/khopland/pgtasks/pause/postgres/db", List.of("V001__topic_pause.sql"), List.of("topic_pause"));

    private final PostgresTaskStore store;
    private final String table;
    private volatile boolean schemaReady;

    /** Created by the capability for a particular store. */
    PostgresPauseStore(PostgresTaskStore store) {
        this.store = store;
        table = store.schema() + ".topic_pause";
    }

    @Override
    public void prepare() {
        try {
            ensureSchema();
        } catch (SQLException | RuntimeException e) {
            throw new IllegalStateException("Could not apply " + SCHEMA.describe() + " to " + table
                + ": pausing topics needs its own table", e);
        }
    }

    @Override
    public boolean pause(StoreTransaction transaction, String topic) throws SQLException {
        prepare();
        // The database's clock, as for everything else the store records.
        return update(transaction, "INSERT INTO " + table + " (topic, paused_at) VALUES (?, clock_timestamp()) ON CONFLICT (topic) DO NOTHING", topic);
    }

    @Override
    public boolean resume(StoreTransaction transaction, String topic) throws SQLException {
        prepare();
        return update(transaction, "DELETE FROM " + table + " WHERE topic = ?", topic);
    }

    @Override
    public List<Paused> list(StoreTransaction transaction) throws SQLException {
        prepare();
        try (PreparedStatement statement = transaction.connection().prepareStatement("SELECT topic, paused_at FROM " + table + " ORDER BY topic");
             ResultSet rows = statement.executeQuery()) {
            List<Paused> paused = new ArrayList<>();
            while (rows.next()) {
                paused.add(new Paused(rows.getString(1), rows.getObject(2, OffsetDateTime.class).toInstant()));
            }
            return paused;
        }
    }

    private static boolean update(StoreTransaction transaction, String sql, String topic) throws SQLException {
        try (PreparedStatement statement = transaction.connection().prepareStatement(sql)) {
            statement.setString(1, topic);
            return statement.executeUpdate() > 0;
        }
    }

    /** Applies the extension's files once per store instance, according to the store's schema mode. */
    private synchronized void ensureSchema() throws SQLException {
        if (!schemaReady) {
            SCHEMA.apply(store);
            schemaReady = true;
        }
    }

    @Override
    public String toString() {
        return "PostgresPauseStore[" + table + "]";
    }
}
