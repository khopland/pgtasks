package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.random.RandomGenerator;

import org.jspecify.annotations.Nullable;

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
 *                    for never, which a time budget also has
 * @param initial     backoff after the first failure
 * @param multiplier  growth per further failure
 * @param max         upper bound for the backoff
 * @param jitter      fraction of the backoff that is randomized, 0 to 1: a backoff is up to this
 *                    much shorter, so tasks that failed together don't retry together
 * @param budget      how long a task keeps trying after its first failure, if the policy is a time
 *                    budget; null if it is a number of attempts. A counted failure dead-letters
 *                    the task once its next retry would start past the budget. The clock runs
 *                    from the first failure of any kind, so a task that failed through a long
 *                    outage without using attempts gives up at its first counted failure after
 *                    the budget
 * @param notCounting failures caused by these types, anywhere in the cause chain, are the
 *                    system's, not the task's: retried with the backoff, the attempt not counted
 * @param deadLetterAt failures caused by these types go straight to the dead letters, such as
 *                    bad input that no retry will fix. Checked before {@code notCounting}
 */
public record Retry(int maxAttempts, Duration initial, double multiplier, Duration max, double jitter, @Nullable Duration budget,
                    List<Class<? extends Throwable>> notCounting, List<Class<? extends Throwable>> deadLetterAt) {

    public static final int FOREVER = Integer.MAX_VALUE;

    /** What a failure means for its task. */
    public enum Kind {
        /** Uses up an attempt. */
        COUNTED,
        /** Retried without using up an attempt. */
        NOT_COUNTED,
        /** Dead-lettered at once. */
        DEAD_LETTER
    }

    public Retry {
        Objects.requireNonNull(initial, "initial");
        Objects.requireNonNull(max, "max");
        notCounting = List.copyOf(notCounting);
        deadLetterAt = List.copyOf(deadLetterAt);
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
            maxAttempts = FOREVER;
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
        return new Retry(FOREVER, first, 2, cap, 0.2, Duration.ofDays(7), List.of(), List.of());
    }

    /** Dead-letters a task at its first failure. */
    public static Retry none() {
        return new Retry(1, Duration.ZERO, 1, Duration.ZERO, 0, null, List.of(), List.of());
    }

    /** Keeps trying for this long after the first failure, then dead-letters the task. */
    public Retry giveUpAfter(Duration budget) {
        return new Retry(1, initial, multiplier, max, jitter, Objects.requireNonNull(budget, "budget"), notCounting, deadLetterAt);
    }

    /** Never gives up: a failing task retries at the capped backoff until it succeeds. */
    public Retry forever() {
        return new Retry(FOREVER, initial, multiplier, max, jitter, null, notCounting, deadLetterAt);
    }

    /** Gives up after this many counted attempts. */
    public Retry attempts(int maxAttempts) {
        return new Retry(maxAttempts, initial, multiplier, max, jitter, null, notCounting, deadLetterAt);
    }

    /** How much each backoff grows over the one before, 2 by default. */
    public Retry withMultiplier(double multiplier) {
        return new Retry(maxAttempts, initial, multiplier, max, jitter, budget, notCounting, deadLetterAt);
    }

    /** The share of each backoff that is random, 0 to 1; 0.2 by default. */
    public Retry withJitter(double jitter) {
        return new Retry(maxAttempts, initial, multiplier, max, jitter, budget, notCounting, deadLetterAt);
    }

    /** Another ramp, keeping how the policy gives up; a time budget is worked out again. */
    public Retry withBackoff(Duration first, Duration cap) {
        return new Retry(maxAttempts, first, multiplier, cap, jitter, budget, notCounting, deadLetterAt);
    }

    /**
     * Failures caused by these types don't use up an attempt: a connection refused, a timeout,
     * anything that says a downstream system is down rather than the task is wrong.
     */
    @SafeVarargs
    public final Retry notCounting(Class<? extends Throwable>... types) {
        return new Retry(maxAttempts, initial, multiplier, max, jitter, budget, append(notCounting, types), deadLetterAt);
    }

    /** Failures caused by these types dead-letter the task at once, such as bad input. */
    @SafeVarargs
    public final Retry deadLetterAt(Class<? extends Throwable>... types) {
        return new Retry(maxAttempts, initial, multiplier, max, jitter, budget, notCounting, append(deadLetterAt, types));
    }

    /**
     * What a failure with this cause means, by the types anywhere in its cause chain. A payload
     * the codec can't decode is always dead-lettered at once: retrying won't change it.
     */
    public Kind classify(@Nullable Throwable cause) {
        if (cause instanceof UndecodablePayload || matches(cause, deadLetterAt)) {
            return Kind.DEAD_LETTER;
        }
        return matches(cause, notCounting) ? Kind.NOT_COUNTED : Kind.COUNTED;
    }

    /**
     * The backoff after an uncounted failure. A policy without a backoff, such as {@link #none()},
     * would retry at once, again and again, so those back off as the standard policy does.
     */
    public Duration uncountedBackoff(int failures, RandomGenerator random) {
        return initial.isZero() ? standard().backoff(failures, random) : backoff(failures, random);
    }

    private static boolean matches(@Nullable Throwable cause, List<Class<? extends Throwable>> types) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable current = cause; current != null && seen.add(current); current = current.getCause()) {
            for (Class<? extends Throwable> type : types) {
                if (type.isInstance(current)) {
                    return true;
                }
            }
        }
        return false;
    }

    @SafeVarargs
    private static List<Class<? extends Throwable>> append(List<Class<? extends Throwable>> list, Class<? extends Throwable>... more) {
        List<Class<? extends Throwable>> all = new ArrayList<>(list);
        for (Class<? extends Throwable> type : more) {
            all.add(Objects.requireNonNull(type, "type"));
        }
        return all;
    }

    public boolean isForever() {
        return maxAttempts == FOREVER && budget == null;
    }

    /**
     * Whether a counted failure of this attempt is retried after {@code backoff}: attempts are
     * left, and with a time budget, the retry starts within it.
     *
     * @param firstFailedAt when the task first failed, or null if this is its first failure
     */
    public boolean allowsAnotherAfter(int attempt, @Nullable Instant firstFailedAt, Instant now, Duration backoff) {
        if (attempt >= maxAttempts) {
            return false;
        }
        if (budget == null) {
            return true;
        }
        Instant start = firstFailedAt == null ? now : firstFailedAt;
        return !now.plus(backoff).isAfter(start.plus(budget));
    }

    /**
     * About how many attempts a time budget allows when every run fails at once at the nominal
     * backoff; {@link #maxAttempts} otherwise.
     */
    public int nominalAttempts() {
        return budget == null ? maxAttempts : attemptsWithin(budget, initial, multiplier, max);
    }

    /** The backoff after the given failed attempt, starting at 1. */
    public Duration backoff(int attempt, RandomGenerator random) {
        double nanos = nominal(attempt, initial, multiplier, max);
        if (jitter > 0) {
            nanos = nanos * (1 - jitter) + nanos * jitter * random.nextDouble();
        }
        return Duration.ofNanos((long) nanos);
    }

    /** "for 7 days after the first failure (about 343 attempts), from PT10S x2.0 up to PT30M", for the effective settings. */
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
            return "for " + time + " after the first failure (about " + nominalAttempts() + " attempts), " + ramp;
        }
        return maxAttempts + " attempts, " + ramp;
    }

    private static double nominal(int attempt, Duration initial, double multiplier, Duration max) {
        double nanos = (double) initial.toNanos() * Math.pow(multiplier, Math.max(0, attempt - 1));
        return Math.min(nanos, (double) max.toNanos());
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
