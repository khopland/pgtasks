package io.github.khopland.pgtasks.engine;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;

import io.github.khopland.pgtasks.engine.extension.ExtensionData;
import io.github.khopland.pgtasks.spi.ExtensionEntry;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.jspecify.annotations.Nullable;

/**
 * One extension's entries in the store; see {@link ExtensionData}. A write tells the extension's
 * data reloads on every node, on the engine's {@value #CHANNEL} channel, when the extension has
 * any: an extension without them writes without the notification's cost at commit.
 */
final class StoredData implements ExtensionData {

    /** The engine's channel for changed entries; its payload is {@link #payload}. */
    static final String CHANNEL = "data";

    /** Entries read per statement by {@link #list}. */
    private static final int PAGE = 500;
    /** Rounds of {@link #update} before it gives up on a key others keep writing. */
    private static final int UPDATE_ROUNDS = 100;
    /** Postgres refuses a notification payload of 8000 bytes or more. */
    private static final int MAX_PAYLOAD_BYTES = 7_000;

    private final TaskStore store;
    private final String extension;
    private final BooleanSupplier announced;

    /** @param announced whether the extension has data reloads, so its writes are announced */
    StoredData(TaskStore store, String extension, BooleanSupplier announced) {
        this.store = store;
        this.extension = extension;
        this.announced = announced;
    }

    @Override
    public Optional<ExtensionEntry> get(StoreTransaction transaction, String key) throws SQLException {
        return store.extensionEntry(Objects.requireNonNull(transaction, "transaction"), extension, key(key));
    }

    @Override
    public List<ExtensionEntry> list(StoreTransaction transaction, String prefix) throws SQLException {
        Objects.requireNonNull(transaction, "transaction");
        if (Objects.requireNonNull(prefix, "prefix").length() > ExtensionEntry.MAX_KEY) {
            return List.of();
        }
        List<ExtensionEntry> entries = new ArrayList<>();
        String after = null;
        while (true) {
            List<ExtensionEntry> page = store.extensionEntries(transaction, extension, prefix, after, PAGE);
            entries.addAll(page);
            if (page.size() < PAGE) {
                return entries;
            }
            after = page.getLast().key();
        }
    }

    @Override
    public ExtensionEntry put(StoreTransaction transaction, String key, String value) throws SQLException {
        ExtensionEntry written = store.putExtensionEntry(Objects.requireNonNull(transaction, "transaction"), extension, key(key), value(value), null)
            .orElseThrow(() -> new IllegalStateException("The store wrote no entry for " + key));
        changed(transaction, key);
        return written;
    }

    @Override
    public Optional<ExtensionEntry> putIf(StoreTransaction transaction, String key, long version, String value) throws SQLException {
        if (version < 0) {
            throw new IllegalArgumentException("A version is 0, for an entry that doesn't exist yet, or more, not " + version);
        }
        Optional<ExtensionEntry> written = store.putExtensionEntry(Objects.requireNonNull(transaction, "transaction"), extension, key(key),
            value(value), version);
        if (written.isPresent()) {
            changed(transaction, key);
        }
        return written;
    }

    @Override
    public Optional<ExtensionEntry> update(StoreTransaction transaction, String key, UnaryOperator<@Nullable String> change) throws SQLException {
        Objects.requireNonNull(change, "change");
        for (int round = 0; round < UPDATE_ROUNDS; round++) {
            Optional<ExtensionEntry> now = get(transaction, key);
            String changed = change.apply(now.map(ExtensionEntry::value).orElse(null));
            long version = now.map(ExtensionEntry::version).orElse(0L);
            if (changed == null) {
                if (now.isEmpty() || deleteIf(transaction, key, version)) {
                    return Optional.empty();
                }
                continue;
            }
            Optional<ExtensionEntry> written = putIf(transaction, key, version, changed);
            if (written.isPresent()) {
                return written;
            }
            // Another node wrote first and committed: read its value and change that.
        }
        throw new IllegalStateException("Could not update " + extension + "'s entry " + key + ": other writes kept coming first");
    }

    @Override
    public boolean delete(StoreTransaction transaction, String key) throws SQLException {
        boolean deleted = store.deleteExtensionEntry(Objects.requireNonNull(transaction, "transaction"), extension, key(key), null);
        if (deleted) {
            changed(transaction, key);
        }
        return deleted;
    }

    @Override
    public boolean deleteIf(StoreTransaction transaction, String key, long version) throws SQLException {
        boolean deleted = store.deleteExtensionEntry(Objects.requireNonNull(transaction, "transaction"), extension, key(key), version);
        if (deleted) {
            changed(transaction, key);
        }
        return deleted;
    }

    /** Tells every node's data reloads of this extension, once the transaction commits. */
    private void changed(StoreTransaction transaction, String key) throws SQLException {
        if (announced.getAsBoolean()) {
            store.notify(transaction, CHANNEL, payload(extension, key));
        }
    }

    /**
     * The extension's name and the key, told apart by the name's length: {@code 5:Pauseorders}.
     * Without the key, which a reload takes as "anything may have changed", when they wouldn't fit.
     */
    static String payload(String extension, String key) {
        String full = extension.length() + ":" + extension + key;
        return full.getBytes(StandardCharsets.UTF_8).length <= MAX_PAYLOAD_BYTES ? full : extension.length() + ":" + extension;
    }

    /** A changed entry, as a payload names it: the key is empty for "anything may have changed". */
    record Change(String extension, String key) {
    }

    /** The change a payload names, or null for one that isn't a payload. */
    static @Nullable Change parse(String payload) {
        int colon = payload.indexOf(':');
        if (colon < 1) {
            return null;
        }
        try {
            int length = Integer.parseInt(payload.substring(0, colon));
            int end = colon + 1 + length;
            if (length < 1 || end > payload.length()) {
                return null;
            }
            return new Change(payload.substring(colon + 1, end), payload.substring(end));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String key(String key) {
        Objects.requireNonNull(key, "key");
        if (key.isEmpty() || key.length() > ExtensionEntry.MAX_KEY) {
            throw new IllegalArgumentException("A key is 1 to " + ExtensionEntry.MAX_KEY + " characters, not " + key.length());
        }
        return key;
    }

    private static String value(String value) {
        Objects.requireNonNull(value, "value");
        if (value.length() > ExtensionEntry.MAX_VALUE) {
            throw new IllegalArgumentException("A value is at most " + ExtensionEntry.MAX_VALUE + " characters, not " + value.length());
        }
        return value;
    }

    @Override
    public String toString() {
        return "ExtensionData[" + extension + "]";
    }
}
