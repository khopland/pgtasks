package io.github.khopland.pgtasks;

import java.time.Duration;
import java.time.Instant;

import org.jspecify.annotations.Nullable;

/**
 * Bounds on how long a task can be made to wait. A store adds a delay to its clock, and Postgres
 * holds times only up to the year 294276, so an unbounded one, such as
 * {@code Duration.ofSeconds(Long.MAX_VALUE)} or {@code Instant.MAX} meant as "never", would fail
 * the write: for a handler's decision or downstream work, the completion of its whole batch.
 * Refused where it is made, it fails only the call that made it. To wait for something rather
 * than a time, suspend the task.
 */
final class Waits {

    /** The longest delay: a thousand years. */
    static final Duration MAX_DELAY = Duration.ofDays(365_250);

    /** The run times a task can be given: the years 1 to 9999. */
    static final Instant EARLIEST = Instant.parse("0001-01-01T00:00:00Z");
    static final Instant LATEST = Instant.parse("9999-12-31T23:59:59Z");

    private Waits() {
    }

    static void checkDelay(String what, @Nullable Duration delay) {
        if (delay == null) {
            return;
        }
        if (delay.isNegative()) {
            throw new IllegalArgumentException(what + " must not be negative");
        }
        if (delay.compareTo(MAX_DELAY) > 0) {
            throw new IllegalArgumentException(what + " must be at most a thousand years (" + MAX_DELAY + "), not " + delay
                + "; to wait for something rather than a time, suspend the task");
        }
    }

    static void checkRunAt(@Nullable Instant runAt) {
        if (runAt != null && (runAt.isBefore(EARLIEST) || runAt.isAfter(LATEST))) {
            throw new IllegalArgumentException("runAt must be within the years 1 to 9999, not " + runAt
                + "; to wait for something rather than a time, suspend the task");
        }
    }
}
