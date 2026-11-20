package org.example.task2.engine;

import java.util.Objects;

/**
 * A cluster-wide cap on how many tasks are claimed. Strict, the default, keeps one token bucket in
 * the store, charged in the claiming transaction: over any interval t, at most
 * {@code burst + perSecond * t} tasks are claimed across every node. Approximate splits the rate
 * across live nodes, each with a bucket of its own: no write per claim, but it can overshoot while
 * nodes join or leave. Permits are taken before polling, and empty polls aren't charged.
 *
 * @param name a limit can be shared by several topics, named after a topic or a downstream system
 */
public record RateLimit(String name, double perSecond, int burst, boolean approximate) {

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
        return new RateLimit(name, perSecond, burst, false);
    }

    public RateLimit withRate(double perSecond, int burst) {
        return new RateLimit(name, perSecond, burst, approximate);
    }
}
