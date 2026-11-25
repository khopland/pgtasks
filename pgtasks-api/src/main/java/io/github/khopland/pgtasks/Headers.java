package io.github.khopland.pgtasks;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/** Checks and copies a task's headers: names are not empty, nothing is null or holds a NUL, order is kept. */
final class Headers {

    private Headers() {
    }

    static Map<String, String> copy(Map<String, String> headers) {
        Objects.requireNonNull(headers, "headers");
        if (headers.isEmpty()) {
            return Map.of();
        }
        Map<String, String> copy = new LinkedHashMap<>();
        headers.forEach((name, value) -> {
            Objects.requireNonNull(name, "header name");
            Objects.requireNonNull(value, () -> "value of header " + name);
            if (name.isEmpty()) {
                throw new IllegalArgumentException("header names must not be empty");
            }
            checkText("header name", name);
            checkText("value of header " + name, value);
            copy.put(name, value);
        });
        return Collections.unmodifiableMap(copy);
    }

    /**
     * Refuses text with a NUL character, which Postgres text can't store: pushed, it would fail
     * the push, and staged by a handler, the completion of the handler's whole batch. Refused here,
     * it fails only the call that made it.
     */
    static void checkText(String what, @Nullable String text) {
        if (text != null && text.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(what + " must not contain the NUL character (\\u0000), which the store can't hold");
        }
    }

    /**
     * Refuses a key longer than {@link Creation#MAX_KEY_BYTES} in UTF-8, besides what
     * {@link #checkText} refuses. Postgres indexes identifiers, references and dedup keys, and a
     * B-tree entry can't be much over 2.7 KB: a longer key would fail the push, and staged by a
     * handler, the completion of its whole batch.
     */
    static void checkKey(String what, @Nullable String key) {
        checkText(what, key);
        if (key != null && key.length() > Creation.MAX_KEY_BYTES / 3) {
            int bytes = utf8Length(key);
            if (bytes > Creation.MAX_KEY_BYTES) {
                throw new IllegalArgumentException(what + " is " + bytes + " bytes in UTF-8, and the store indexes it, so it takes at most "
                    + Creation.MAX_KEY_BYTES + "; use a shorter key, such as a hash of this one");
            }
        }
    }

    private static int utf8Length(String text) {
        int bytes = 0;
        for (int index = 0; index < text.length(); index++) {
            char c = text.charAt(index);
            if (c < 0x80) {
                bytes += 1;
            } else if (c < 0x800) {
                bytes += 2;
            } else if (Character.isHighSurrogate(c) && index + 1 < text.length() && Character.isLowSurrogate(text.charAt(index + 1))) {
                bytes += 4;
                index++;
            } else {
                bytes += 3;
            }
        }
        return bytes;
    }
}
