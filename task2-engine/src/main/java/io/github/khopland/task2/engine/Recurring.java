package io.github.khopland.task2.engine;

import java.util.Objects;

import io.github.khopland.task2.Insertion;
import io.github.khopland.task2.TopicRef;

/**
 * A task pushed on a schedule: see {@link TaskRegistry#recurring}.
 *
 * @param name       unique within the topic; renaming one is removing it and adding another
 * @param identifier the pushed task's identifier, the same every time
 * @param payload    the pushed task's payload, the same every time
 * @param insertion  how each push treats an earlier one still waiting; {@code SUPERSEDE} by
 *                   default, so runs don't pile up while the topic is paused or behind
 * @param module     the configurer that registered it
 */
public record Recurring<T>(TopicRef<T> topic, String name, Cron cron, String identifier, T payload, Insertion insertion, String module) {

    public Recurring {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(cron, "cron");
        Objects.requireNonNull(identifier, "identifier");
        Objects.requireNonNull(insertion, "insertion");
        if (name.isBlank()) {
            throw new IllegalArgumentException("A recurring task needs a name");
        }
    }
}
