package org.example.task2;

import java.util.List;

/** Pushes tasks. Which transaction the push joins depends on how the writer was obtained. */
public interface TaskWriter {

    default <T> Pushed push(TopicRef<T> topic, String identifier, T payload) {
        return push(topic, Insertion.APPEND, List.of(Creation.of(identifier, payload))).getFirst();
    }

    default <T> Pushed push(TopicRef<T> topic, String identifier, T payload, Insertion insertion) {
        return push(topic, insertion, List.of(Creation.of(identifier, payload))).getFirst();
    }

    default <T> Pushed push(TopicRef<T> topic, Insertion insertion, Creation<T> creation) {
        return push(topic, insertion, List.of(creation)).getFirst();
    }

    /** Pushes all creations in one statement, in order. Returns one result per creation. */
    <T> List<Pushed> push(TopicRef<T> topic, Insertion insertion, List<Creation<T>> creations);
}
