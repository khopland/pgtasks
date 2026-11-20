package org.example.task2;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A typed reference to a topic. Producers in other modules push through the owning module's
 * {@code TopicRef} constant instead of a string, so the dependency is visible in code.
 *
 * @param name        the topic name, lower case letters, digits, '.', '_' and '-'
 * @param payloadType the payload type of every task in the topic
 */
public record TopicRef<T>(String name, Class<T> payloadType) {

    private static final Pattern NAME = Pattern.compile("[a-z0-9][a-z0-9._-]{0,199}");

    public TopicRef {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(payloadType, "payloadType");
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid topic name '" + name + "': use 1-200 of [a-z0-9._-], starting with a letter or digit");
        }
    }

    public static <T> TopicRef<T> of(String name, Class<T> payloadType) {
        return new TopicRef<>(name, payloadType);
    }

    @Override
    public String toString() {
        return name;
    }
}
