package io.github.khopland.pgtasks.pause;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import io.github.khopland.pgtasks.engine.extension.ExtensionContext;
import io.github.khopland.pgtasks.spi.StoreTransaction;

/**
 * Where {@link Pause} keeps the paused topics, shared by every node. A provider is found with
 * {@link ExtensionContext#capability(Class)}: this module has one for the in-memory store, and
 * {@code pgtasks-pause-postgres} one for {@code pgtasks-postgres}.
 *
 * <p>Every method runs in a transaction the extension opened on the engine's store, so a pause
 * and the message that tells the other nodes commit together. Rows are keyed by topic name.
 */
public interface PauseStore {

    /**
     * Readies the storage before first use, such as applying or checking its tables, and throws if
     * it can't be used. Called each time the engine starts; nothing by default.
     */
    default void prepare() {
    }

    /** Marks a topic paused, from the store's clock; false if it already was. */
    boolean pause(StoreTransaction transaction, String topic) throws SQLException;

    /** Marks a topic no longer paused; false if it wasn't. */
    boolean resume(StoreTransaction transaction, String topic) throws SQLException;

    /** Every paused topic, by name. */
    List<Paused> list(StoreTransaction transaction) throws SQLException;

    /** A paused topic and when it was paused. */
    record Paused(String topic, Instant pausedAt) {
        public Paused {
            Objects.requireNonNull(topic, "topic");
            Objects.requireNonNull(pausedAt, "pausedAt");
        }
    }
}
