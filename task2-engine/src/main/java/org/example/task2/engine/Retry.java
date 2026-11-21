package org.example.task2.engine;

import java.time.Duration;
import java.util.Objects;
import java.util.random.RandomGenerator;

/**
 * How failed attempts are retried: exponential backoff with jitter, until the task has used up
 * its attempts or its time budget, or forever. A task that gives up is dead-lettered: it stays
 * failed in the queue until someone revives or discards it.
 *
 * <pre>
 * Retry.standard()                                                   // 10 s doubling to 30 min, gives up after 7 days
 * Retry.exponential(Duration.ofSeconds(5), Duration.ofMinutes(10))   // the same budget, another ramp
 *     .giveUpAfter(Duration.ofDays(1))
 * Retry.exponential(Duration.ofSeconds(10), Duration.ofMinutes(30)).forever()   // 1.x: never gives up
 * Retry.none()                                                       // dead letter at the first failure
 * </pre>
 *
 * A short outage then costs a few quick retries, a long one a retry every half hour, and a task
 * that fails because of a bug runs again once the fix is deployed, as long as that is within the
 * budget.
 *
 * @param maxAttempts counted attempts before the task is dead-lettered; {@link Integer#MAX_VALUE}
 *                    for never
 * @param initial     backoff after the first failure
 * @param multiplier  growth per further failure
 * @param max         upper bound for the backoff
 * @param jitter      fraction of the backoff that is randomized, 0 to 1: a backoff is up to this
 *                    much shorter, so tasks that failed together don't retry together
 * @param budget      how long a task keeps trying, if the policy is a time budget; null if it is a
 *                    number of attempts. {@code maxAttempts} is worked out from it: the attempts
 *                    whose nominal backoffs fit in the budget
 */
public record Retry(int maxAttempts, Duration initial, double multiplier, Duration max, double jitter, Duration budget) {

    public static final int FOREVER = Integer.MAX_VALUE;

    public Retry(int maxAttempts, Duration initial, double multiplier, Duration max, double jitter) {
        this(maxAttempts, initial, multiplier, max, jitter, null);
    }

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
        if (budget != null) {
            if (!budget.isPositive()) {
                throw new IllegalArgumentException("a time budget must be positive");
            }
            if (initial.isZero()) {
                throw new IllegalArgumentException("a time budget needs a backoff above zero");
            }
            maxAttempts = attemptsWithin(budget, initial, multiplier, max);
        }
    }

    /** 10 s, doubling to a 30 min cap, with 20 % jitter; gives up after 7 days (about 340 attempts). */
    public static Retry standard() {
        return exponential(Duration.ofSeconds(10), Duration.ofMinutes(30));
    }

    /**
     * Backoff from {@code first}, doubling up to {@code cap}, with 20 % jitter, giving up after 7
     * days unless changed with {@link #giveUpAfter}, {@link #forever()} or {@link #attempts}.
     */
    public static Retry exponential(Duration first, Duration cap) {
        return new Retry(1, first, 2, cap, 0.2, Duration.ofDays(7));
    }

    /** Dead-letters a task at its first failure. */
    public static Retry none() {
        return new Retry(1, Duration.ZERO, 1, Duration.ZERO, 0);
    }

    /** Keeps trying for this long after the first failure, then dead-letters the task. */
    public Retry giveUpAfter(Duration budget) {
        return new Retry(1, initial, multiplier, max, jitter, Objects.requireNonNull(budget, "budget"));
    }

    /** Never gives up: a failing task retries at the capped backoff until it succeeds. */
    public Retry forever() {
        return new Retry(FOREVER, initial, multiplier, max, jitter, null);
    }

    /** Gives up after this many counted attempts. */
    public Retry attempts(int maxAttempts) {
        return new Retry(maxAttempts, initial, multiplier, max, jitter, null);
    }

    /** Same as {@link #attempts}. */
    public Retry withMaxAttempts(int maxAttempts) {
        return attempts(maxAttempts);
    }

    /** Another ramp, keeping how the policy gives up; a time budget is worked out again. */
    public Retry withBackoff(Duration first, Duration cap) {
        return new Retry(maxAttempts, first, multiplier, cap, jitter, budget);
    }

    public boolean isForever() {
        return maxAttempts == FOREVER;
    }

    /** Whether a failure in this attempt still leaves an attempt to retry with. */
    public boolean allowsAnotherAfter(int attempt) {
        return attempt < maxAttempts;
    }

    /** The backoff after the given failed attempt, starting at 1. */
    public Duration backoff(int attempt, RandomGenerator random) {
        double nanos = nominal(attempt, initial, multiplier, max);
        if (jitter > 0) {
            nanos = nanos * (1 - jitter) + nanos * jitter * random.nextDouble();
        }
        return Duration.ofNanos((long) nanos);
    }

    /** "for 7 days (343 attempts), from PT10S x2.0 up to PT30M", for the effective settings. */
    public String describe() {
        if (maxAttempts == 1) {
            return "none: dead letter at the first failure";
        }
        String ramp = "from " + initial + " x" + multiplier + " up to " + max;
        if (isForever()) {
            return "forever, " + ramp;
        }
        if (budget != null) {
            String time = budget.toSeconds() % 86_400 == 0 ? budget.toDays() + (budget.toDays() == 1 ? " day" : " days") : budget.toString();
            return "for " + time + " (" + maxAttempts + " attempts), " + ramp;
        }
        return maxAttempts + " attempts, " + ramp;
    }

    private static double nominal(int attempt, Duration initial, double multiplier, Duration max) {
        double nanos = initial.toNanos() * Math.pow(multiplier, Math.max(0, attempt - 1));
        return Math.min(nanos, max.toNanos());
    }

    /** The attempts whose nominal backoffs, added up, fit in the budget: the first plus each retry. */
    private static int attemptsWithin(Duration budget, Duration initial, double multiplier, Duration max) {
        double left = budget.toNanos();
        long attempts = 1;
        while (true) {
            double next = nominal((int) Math.min(attempts, Integer.MAX_VALUE), initial, multiplier, max);
            if (next >= max.toNanos()) {
                // At the cap: every further retry waits as long, so count them at once.
                attempts += (long) (left / next);
                return (int) Math.min(attempts, FOREVER - 1);
            }
            if (next > left) {
                return (int) attempts;
            }
            left -= next;
            attempts++;
        }
    }
}
