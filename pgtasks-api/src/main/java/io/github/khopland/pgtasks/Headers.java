package io.github.khopland.pgtasks;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Checks and copies a task's headers: names are not empty, nothing is null, order is kept. */
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
            copy.put(name, value);
        });
        return Collections.unmodifiableMap(copy);
    }
}
