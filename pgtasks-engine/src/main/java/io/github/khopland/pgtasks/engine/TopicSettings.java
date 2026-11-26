package io.github.khopland.pgtasks.engine;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;

import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.Outcome;


/**
 * Per-topic settings. {@link #defaults()} is safe in production; change only what differs.
 *
 * @param batchSize     most tasks claimed per batch
 * @param concurrency   batches in flight per topic on one node
 * @param ordering      per-identifier ordering
 * @param retry         what happens to failed attempts
 * @param batchDeadline a batch whose handler runs longer is cancelled: what its deciders decided
 *                      by then stands, the tasks still being decided fail, and those not started
 *                      go back without an attempt
 * @param pollInterval  fallback poll when no notification arrives
 * @param retention     how long finished tasks are kept per outcome, counted from completion, from
 *                      zero to a thousand years; an outcome that isn't listed is kept forever
 */
record TopicSettings(
    int batchSize,
    int concurrency,
    Ordering ordering,
    Retry retry,
    Duration batchDeadline,
    Duration pollInterval,
    Map<Outcome, Period> retention
) {

    /** Succeeded 30 days, filtered, redundant and recreated 7 days, failed forever. */
    public static final Map<Outcome, Period> DEFAULT_RETENTION = Map.of(
        Outcome.SUCCEEDED, Period.ofDays(30),
        Outcome.FILTERED, Period.ofDays(7),
        Outcome.REDUNDANT, Period.ofDays(7),
        Outcome.RECREATED, Period.ofDays(7)
    );

    public TopicSettings {
        Objects.requireNonNull(ordering, "ordering");
        Objects.requireNonNull(retry, "retry");
        Objects.requireNonNull(batchDeadline, "batchDeadline");
        Objects.requireNonNull(pollInterval, "pollInterval");
        if (batchSize < 1 || concurrency < 1) {
            throw new IllegalArgumentException("batchSize and concurrency must be at least 1");
        }
        if (!batchDeadline.isPositive() || !pollInterval.isPositive()) {
            throw new IllegalArgumentException("batchDeadline and pollInterval must be positive");
        }
        retention.forEach((outcome, period) -> checkRetention(outcome, period));
        retention = Map.copyOf(retention);
    }

    /** The longest retention, as long as the longest delay: whatever is kept longer is kept forever in practice. */
    static final Period MAX_RETENTION = Period.ofYears(1_000);

    /**
     * Refuses a retention the housekeeping leader can't apply. A negative one would delete every
     * finished task of the outcome at once, and one too long to subtract from the date stopped
     * the retention of every topic after it, on every run. Keeping an outcome forever is
     * {@code retainForever}.
     */
    static Period checkRetention(Outcome outcome, Period period) {
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(period, () -> "retention of " + outcome.label());
        if (period.isNegative()) {
            throw new IllegalArgumentException("retention of " + outcome.label() + " must not be negative, not " + period);
        }
        LocalDate from = LocalDate.of(2000, 1, 1);
        boolean tooLong;
        try {
            tooLong = ChronoUnit.DAYS.between(from, from.plus(period)) > ChronoUnit.DAYS.between(from, from.plus(MAX_RETENTION));
        } catch (DateTimeException | ArithmeticException e) {
            tooLong = true;
        }
        if (tooLong) {
            throw new IllegalArgumentException("retention of " + outcome.label() + " must be at most a thousand years (" + MAX_RETENTION + "), not "
                + period + "; to keep it forever, use retainForever");
        }
        return period;
    }

    /** 100 tasks per batch, 2 batches per node, FIFO, standard retry, 30 min deadline, 30 s poll. */
    public static TopicSettings defaults() {
        return new TopicSettings(100, 2, Ordering.FIFO, Retry.standard(), Duration.ofMinutes(30), Duration.ofSeconds(30), DEFAULT_RETENTION);
    }

    public TopicSettings withBatchSize(int batchSize) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, retention);
    }

    public TopicSettings withConcurrency(int concurrency) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, retention);
    }

    public TopicSettings withOrdering(Ordering ordering) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, retention);
    }

    public TopicSettings withRetry(Retry retry) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, retention);
    }

    public TopicSettings withBatchDeadline(Duration batchDeadline) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, retention);
    }

    public TopicSettings withPollInterval(Duration pollInterval) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, retention);
    }

    public TopicSettings withRetention(Map<Outcome, Period> retention) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, retention);
    }

}
