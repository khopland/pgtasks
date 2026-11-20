package org.example.task2.engine;

import java.time.Duration;
import java.util.Objects;
import java.util.random.RandomGenerator;

/**
 * How failed attempts are retried: exponential backoff with jitter, up to a number of attempts.
 * After the last one the task is dead-lettered: it stays failed in the queue until someone
 * revives or discards it.
 *
 * @param maxAttempts counted attempts before the task is dead-lettered
 * @param initial     backoff after the first failure
 * @param multiplier  growth per further failure
 * @param max         upper bound for the backoff
 * @param jitter      fraction of the backoff that is randomized, 0 to 1
 */
public record Retry(int maxAttempts, Duration initial, double multiplier, Duration max, double jitter) {

    public Retry {
        Objects.requireNonNull(initial, "initial");
        Objects.requireNonNull(max, "max");
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        if (initial.isNegative() || max.compareTo(initial) < 0) {
            throw new IllegalArgumentException("need 0 <= initial <= max");
        }
        if (multiplier < 1) {
            throw new IllegalArgumentException("multiplier must be at least 1");
        }
        if (jitter < 0 || jitter > 1) {
            throw new IllegalArgumentException("jitter must be between 0 and 1");
        }
    }

    /** 5 attempts, exponential from 10 s, doubling up to 1 h, with jitter. */
    public static Retry standard() {
        return new Retry(5, Duration.ofSeconds(10), 2, Duration.ofHours(1), 0.2);
    }

    /** Retries forever at a fixed interval, like 1.x topics that reset failed tasks periodically. */
    public static Retry unbounded(Duration interval) {
        return new Retry(Integer.MAX_VALUE, interval, 1, interval, 0);
    }

    public static Retry none() {
        return new Retry(1, Duration.ZERO, 1, Duration.ZERO, 0);
    }

    public Retry withMaxAttempts(int maxAttempts) {
        return new Retry(maxAttempts, initial, multiplier, max, jitter);
    }

    /** Whether a failure in this attempt still leaves an attempt to retry with. */
    public boolean allowsAnotherAfter(int attempt) {
        return attempt < maxAttempts;
    }

    /** The backoff after the given failed attempt, starting at 1. */
    public Duration backoff(int attempt, RandomGenerator random) {
        double nanos = initial.toNanos() * Math.pow(multiplier, Math.max(0, attempt - 1));
        nanos = Math.min(nanos, max.toNanos());
        if (jitter > 0) {
            nanos = nanos * (1 - jitter) + nanos * jitter * random.nextDouble();
        }
        return Duration.ofNanos((long) nanos);
    }
}
