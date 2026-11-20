package org.example.task2.engine;

import java.util.Objects;

/**
 * A strict, cluster-wide cap on how many tasks are claimed: a token bucket kept in the database
 * and charged in the claiming transaction. Over any interval t, at most
 * {@code burst + perSecond * t} tasks are claimed across every node. Permits are taken before
 * polling, and empty polls aren't charged.
 *
 * @param name a limit can be shared by several topics; by default use the topic name
 */
public record RateLimit(String name, double perSecond, int burst) {

    public RateLimit {
        Objects.requireNonNull(name, "name");
        if (!(perSecond > 0)) {
            throw new IllegalArgumentException("rate must be positive; a rate of 0 is not a pause");
        }
        if (burst < 1) {
            throw new IllegalArgumentException("burst must be at least 1");
        }
    }

    public static RateLimit perSecond(String name, double perSecond, int burst) {
        return new RateLimit(name, perSecond, burst);
    }
}
