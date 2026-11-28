package io.github.khopland.pgtasks.engine.extension;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import io.github.khopland.pgtasks.spi.ExtensionEntry;
import io.github.khopland.pgtasks.spi.StoreTransaction;

import org.jspecify.annotations.Nullable;

/**
 * Values an extension keeps in the engine's store, by key: what it knows that must outlast a
 * restart or be the same on every node, such as recurring schedules or a quota's counts. The same
 * on every store, so an extension that keeps its state here needs no table, migration or code per
 * store of its own. From {@link ExtensionContext#data()}.
 *
 * <p>Entries are kept under the extension's {@linkplain Extension#name() name}: every engine on the
 * store, every node and every application on the same database shares them. Each call runs in a
 * transaction the extension holds, such as one from {@link ExtensionContext#transact}, so the
 * writes commit or roll back with everything else in it.
 *
 * <p>A key is up to {@value ExtensionEntry#MAX_KEY} characters, a value up to
 * {@value ExtensionEntry#MAX_VALUE} characters of text the extension encodes as it likes: a
 * number, JSON, or fields of its own. Meant for a modest number of entries, thousands rather than
 * millions: {@link #list} reads all those it matches. Once a write commits, the extension's
 * {@linkplain ExtensionContext#onDataChange reloads} on every node are told its key.
 *
 * <p>The engine implements this interface; don't.
 */
public interface ExtensionData {

    /** The entry of this key, if there is one. */
    Optional<ExtensionEntry> get(StoreTransaction transaction, String key) throws SQLException;

    /** The entries whose keys start with the prefix, taken literally, in key order by code point; {@code ""} for all. */
    List<ExtensionEntry> list(StoreTransaction transaction, String prefix) throws SQLException;

    /** Writes the value, whatever was there. */
    ExtensionEntry put(StoreTransaction transaction, String key, String value) throws SQLException;

    /**
     * Writes the value only if the entry is at this version, 0 for an entry that doesn't exist
     * yet: for a change worked out from what was read, which mustn't overwrite a write another
     * node made in between. Empty if the entry wasn't at that version; read it again and retry,
     * or use {@link #update}.
     */
    Optional<ExtensionEntry> putIf(StoreTransaction transaction, String key, long version, String value) throws SQLException;

    /**
     * Works out the entry's new value from its value now (null if there is none) and writes it,
     * again from the newer value if another node wrote in between: a counter, say. A change that
     * returns null deletes the entry.
     *
     * @return the entry as written, or empty if the change deleted it or there was none to delete
     */
    Optional<ExtensionEntry> update(StoreTransaction transaction, String key, UnaryOperator<@Nullable String> change) throws SQLException;

    /** Deletes the entry. @return whether there was one */
    boolean delete(StoreTransaction transaction, String key) throws SQLException;

    /** Deletes the entry only if it is at this version. @return whether it did */
    boolean deleteIf(StoreTransaction transaction, String key, long version) throws SQLException;
}
