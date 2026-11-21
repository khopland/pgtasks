package org.example.task2.admin;

import java.time.Instant;
import java.util.Objects;

/**
 * An operator's override of a configured rate limit. A null rate or burst keeps the configured
 * value.
 */
public record LimitOverride(String name, Double perSecond, Integer burst, Instant until, long version, Instant updatedAt, String updatedBy) {

    public LimitOverride {
        Objects.requireNonNull(name, "name");
        if (perSecond != null && !(perSecond > 0)) {
            throw new IllegalArgumentException("rate must be positive; pausing is a separate switch");
        }
        if (burst != null && burst < 1) {
            throw new IllegalArgumentException("burst must be at least 1");
        }
    }

    /** Whether it changes the limit at this time: it sets something and hasn't lapsed. */
    public boolean activeAt(Instant now) {
        return (perSecond != null || burst != null) && (until == null || now.isBefore(until));
    }
}
