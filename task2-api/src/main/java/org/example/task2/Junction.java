package org.example.task2;

import java.util.Objects;
import java.util.Set;

/**
 * A junction staged by a handler. If the staging task continues, the completion checks the group
 * (these identifiers plus the staging task's own) in the target topic and releases its
 * suspended tasks once every identifier has a task that counts.
 */
public record Junction(TopicRef<?> topic, Juncture juncture, Set<String> identifiers) {

    public Junction {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(juncture, "juncture");
        identifiers = Set.copyOf(identifiers);
    }
}
