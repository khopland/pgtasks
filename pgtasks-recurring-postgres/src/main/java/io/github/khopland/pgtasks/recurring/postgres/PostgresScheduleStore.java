package io.github.khopland.pgtasks.recurring.postgres;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.github.khopland.pgtasks.postgres.ExtensionSchema;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.recurring.ScheduleStore;
import io.github.khopland.pgtasks.spi.StoreTransaction;

import org.jspecify.annotations.Nullable;

/**
 * Recurring tasks' schedules in the pgtasks schema, one row per topic and name. A due run is found
 * by the database's clock and locked with {@code FOR UPDATE SKIP LOCKED} until its push commits,
 * so nodes that declare the same schedule never fire one run twice, and never wait for each other.
 * Created by {@link PostgresScheduleCapability}, not directly by applications. Its own table comes
 * from {@link #SCHEMA}, not from the engine's schema files.
 */
public final class PostgresScheduleStore implements ScheduleStore {

    /** This extension's SQL, shipped in its jar rather than added to the engine's schema files. */
    public static final ExtensionSchema SCHEMA = ExtensionSchema.of("recurring", PostgresScheduleStore.class,
        "io/github/khopland/pgtasks/recurring/postgres/db",
        List.of("V001__recurring_schedule.sql"), List.of("recurring_schedule"));

    private static final String COLUMNS = "topic, name, cron, zone, next_run_at, last_run_at, seen_at";

    private final PostgresTaskStore store;
    private final String table;
    private volatile boolean schemaReady;

    /** Created by the capability for a particular store. */
    PostgresScheduleStore(PostgresTaskStore store) {
        this.store = store;
        table = store.schema() + ".recurring_schedule";
    }

    @Override
    public void prepare() {
        try {
            ensureSchema();
        } catch (SQLException | RuntimeException e) {
            throw new IllegalStateException("Could not apply " + SCHEMA.describe() + " to " + table
                + ": recurring tasks need their own table", e);
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
    public void declare(StoreTransaction transaction, List<Declaration> schedules) throws SQLException {
        // An unchanged cron and zone keep the next run, so a restart doesn't move the schedule.
        String sql = "INSERT INTO " + table + " AS s (topic, name, cron, zone, next_run_at, seen_at) "
            + "VALUES (?, ?, ?, ?, ?, clock_timestamp()) "
            + "ON CONFLICT (topic, name) DO UPDATE SET "
            + "next_run_at = CASE WHEN s.cron = excluded.cron AND s.zone = excluded.zone THEN s.next_run_at ELSE excluded.next_run_at END, "
            + "cron = excluded.cron, zone = excluded.zone, seen_at = excluded.seen_at";
        try (PreparedStatement statement = transaction.connection().prepareStatement(sql)) {
            for (Declaration schedule : schedules) {
                statement.setString(1, schedule.key().topic());
                statement.setString(2, schedule.key().name());
                statement.setString(3, schedule.cron());
                statement.setString(4, schedule.zone());
                statement.setObject(5, utc(schedule.nextRunAt()));
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    @Override
    public void seen(StoreTransaction transaction, Collection<Key> keys) throws SQLException {
        Connection connection = transaction.connection();
        String sql = "UPDATE " + table + " SET seen_at = clock_timestamp() "
            + "WHERE (topic, name) IN (SELECT * FROM unnest(?::text[], ?::text[]))";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindKeys(connection, statement, keys);
            statement.executeUpdate();
        }
    }

    @Override
    public List<Row> due(StoreTransaction transaction, Collection<Key> keys, int limit) throws SQLException {
        Connection connection = transaction.connection();
        // Guarded like the core store's claims: a node that goes silent holding a run must not keep
        // it from firing for longer than this.
        String sql = "SELECT " + COLUMNS + ", "
            + "set_config('idle_in_transaction_session_timeout', '" + PostgresTaskStore.STUCK_TRANSACTION_TIMEOUT.toMillis() + "', true) "
            + "FROM " + table + " "
            + "WHERE (topic, name) IN (SELECT * FROM unnest(?::text[], ?::text[])) AND next_run_at <= clock_timestamp() "
            + "ORDER BY next_run_at LIMIT ? FOR UPDATE SKIP LOCKED";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindKeys(connection, statement, keys);
            statement.setInt(3, limit);
            return rows(statement);
        }
    }

    @Override
    public void fired(StoreTransaction transaction, Key key, @Nullable Instant firedAt, Instant nextRunAt) throws SQLException {
        String sql = "UPDATE " + table + " SET next_run_at = ?, last_run_at = coalesce(?, last_run_at) WHERE topic = ? AND name = ?";
        try (PreparedStatement statement = transaction.connection().prepareStatement(sql)) {
            statement.setObject(1, utc(nextRunAt));
            if (firedAt == null) {
                statement.setNull(2, Types.TIMESTAMP_WITH_TIMEZONE);
            } else {
                statement.setObject(2, utc(firedAt));
            }
            statement.setString(3, key.topic());
            statement.setString(4, key.name());
            statement.executeUpdate();
        }
    }

    @Override
    public int forget(StoreTransaction transaction, Instant before) throws SQLException {
        try (PreparedStatement statement = transaction.connection().prepareStatement("DELETE FROM " + table + " WHERE seen_at < ?")) {
            statement.setObject(1, utc(before));
            return statement.executeUpdate();
        }
    }

    @Override
    public List<Row> list(StoreTransaction transaction) throws SQLException {
        try (PreparedStatement statement = transaction.connection().prepareStatement("SELECT " + COLUMNS + " FROM " + table + " ORDER BY topic, name")) {
            return rows(statement);
        }
    }

    private static void bindKeys(Connection connection, PreparedStatement statement, Collection<Key> keys) throws SQLException {
        Array topics = connection.createArrayOf("text", keys.stream().map(Key::topic).toArray());
        Array names = connection.createArrayOf("text", keys.stream().map(Key::name).toArray());
        statement.setArray(1, topics);
        statement.setArray(2, names);
    }

    private static List<Row> rows(PreparedStatement statement) throws SQLException {
        List<Row> rows = new ArrayList<>();
        try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                OffsetDateTime last = resultSet.getObject("last_run_at", OffsetDateTime.class);
                rows.add(new Row(new Key(resultSet.getString("topic"), resultSet.getString("name")), resultSet.getString("cron"),
                    resultSet.getString("zone"), resultSet.getObject("next_run_at", OffsetDateTime.class).toInstant(),
                    last == null ? null : last.toInstant(), resultSet.getObject("seen_at", OffsetDateTime.class).toInstant()));
            }
        }
        return rows;
    }

    private static OffsetDateTime utc(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    @Override
    public String toString() {
        return "PostgresScheduleStore[" + table + "]";
    }
}
