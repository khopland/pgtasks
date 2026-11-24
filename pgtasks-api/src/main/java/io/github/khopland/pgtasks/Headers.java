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
}
