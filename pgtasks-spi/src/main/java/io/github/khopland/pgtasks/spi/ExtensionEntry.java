package io.github.khopland.pgtasks.spi;

import java.time.Instant;
import java.util.Objects;

/**
 * One value an extension keeps in the store, by its key; see
 * {@link TaskStore#extensionEntry}. Extensions get it through the engine's {@code ExtensionData}.
 *
 * @param key       unique per extension, at most {@value #MAX_KEY} characters
 * @param value     text the extension encodes as it likes, at most {@value #MAX_VALUE} characters
 * @param version   1 when the entry is first written and one more on each write, for a write that
 *                  only succeeds if nobody wrote in between
 * @param updatedAt when it was last written, by the store's clock
 */
public record ExtensionEntry(String key, String value, long version, Instant updatedAt) {

    /** The longest key, in characters: short enough to name in a notification. */
    public static final int MAX_KEY = 200;

    /** The longest value, in characters. */
    public static final int MAX_VALUE = 65_536;

    public ExtensionEntry {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(updatedAt, "updatedAt");
        if (version < 1) {
            throw new IllegalArgumentException("An entry's version starts at 1, not " + version);
        }
    }
}
