package io.github.khopland.pgtasks.recurring;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import io.github.khopland.pgtasks.engine.extension.ExtensionContext;
import io.github.khopland.pgtasks.spi.StoreTransaction;

import org.jspecify.annotations.Nullable;

/**
 * Where {@link Recurring} keeps its schedules, shared by every node: when each runs next and when
 * it last ran. A provider is found with {@link ExtensionContext#capability(Class)}: this module has
 * one for the in-memory store, and {@code pgtasks-recurring-postgres} one for {@code pgtasks-postgres}.
 *
 * <p>Every method runs in a transaction the extension opened on the engine's store, so a run's
 * push and its move to the next run commit together. A row is keyed by topic and name, and holds
 * no payload: a node fires only the schedules it declares, from its own declaration.
 */
public interface ScheduleStore {

    /**
     * Readies the storage before first use, such as applying or checking its tables, and throws if
     * it can't be used. Called each time the engine starts; nothing by default.
     */
    default void prepare() {
    }

    /**
     * Records that this node declares these schedules: a new one is added with its {@code nextRunAt},
     * one whose cron or zone changed takes the given {@code nextRunAt}, and an unchanged one keeps
     * its own. Each is marked as seen now.
     */
    void declare(StoreTransaction transaction, List<Declaration> schedules) throws SQLException;

    /** Marks these schedules as seen now, so they aren't forgotten while a node declares them. */
    void seen(StoreTransaction transaction, Collection<Key> keys) throws SQLException;

    /**
     * The schedules among these that are due by the store's clock, earliest first, at most
     * {@code limit}. They stay locked until the transaction ends, and a schedule another transaction
     * holds is skipped, so two nodes never fire one run twice.
     */
    List<Row> due(StoreTransaction transaction, Collection<Key> keys, int limit) throws SQLException;

    /** Moves a schedule on to its next run, recording when it fired; a null {@code firedAt} keeps the last. */
    void fired(StoreTransaction transaction, Key key, @Nullable Instant firedAt, Instant nextRunAt) throws SQLException;

    /** Deletes the schedules no node has seen since {@code before}; returns how many. */
    int forget(StoreTransaction transaction, Instant before) throws SQLException;

    /** Every stored schedule, by topic and name. */
    List<Row> list(StoreTransaction transaction) throws SQLException;

    /** A schedule's key: its topic's name and its own name. */
    record Key(String topic, String name) {
        public Key {
            Objects.requireNonNull(topic, "topic");
            Objects.requireNonNull(name, "name");
        }
    }

    /** A schedule as a node declares it; the cron is read in {@code zone}. */
    record Declaration(Key key, String cron, String zone, Instant nextRunAt) {
        public Declaration {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(cron, "cron");
            Objects.requireNonNull(zone, "zone");
            Objects.requireNonNull(nextRunAt, "nextRunAt");
        }
    }

    /** A stored schedule: the cron and zone are what the node that declared it last gave. */
    record Row(Key key, String cron, String zone, Instant nextRunAt, @Nullable Instant lastRunAt, Instant seenAt) {
        public Row {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(cron, "cron");
            Objects.requireNonNull(zone, "zone");
            Objects.requireNonNull(nextRunAt, "nextRunAt");
            Objects.requireNonNull(seenAt, "seenAt");
        }
    }
}
