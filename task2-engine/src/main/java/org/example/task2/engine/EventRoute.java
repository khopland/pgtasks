package org.example.task2.engine;

import java.util.Objects;
import java.util.function.Function;

import org.example.task2.Push;

/**
 * Turns an application event into a task: see {@link TaskRegistry#onEvent}.
 *
 * @param type   the events it takes, subtypes included
 * @param route  the push for an event, or null to push nothing
 * @param module the configurer that registered it
 */
public record EventRoute<E>(Class<E> type, Function<? super E, Push<?>> route, String module) {

    public EventRoute {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(route, "route");
    }

    /** The push for this event, or null if the route doesn't take it or pushes nothing. */
    Push<?> apply(Object event) {
        return type.isInstance(event) ? route.apply(type.cast(event)) : null;
    }
}
