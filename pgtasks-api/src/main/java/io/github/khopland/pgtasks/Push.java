package io.github.khopland.pgtasks;

import java.util.Objects;

/** Downstream work staged by a handler: committed with the completion, if the task continues. */
public record Push<T>(TopicRef<T> topic, Insertion insertion, Creation<T> creation) {

    public Push {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(insertion, "insertion");
        Objects.requireNonNull(creation, "creation");
    }
}
