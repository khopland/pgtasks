package io.github.khopland.task2.engine;

import java.time.Duration;
import java.time.Period;
import java.util.Map;
import java.util.Objects;

import io.github.khopland.task2.Ordering;
import io.github.khopland.task2.Outcome;

/**
 * Per-topic settings. {@link #defaults()} is safe in production; change only what differs.
 *
 * @param batchSize     most tasks claimed per batch
 * @param concurrency   batches in flight per topic on one node
 * @param ordering      per-identifier ordering
 * @param retry         what happens to failed attempts
 * @param batchDeadline a batch whose handler runs longer is cancelled and its tasks retried
 * @param pollInterval  fallback poll when no notification arrives
 * @param rateLimit     optional admission limit
 * @param retention     how long finished tasks are kept per outcome, counted from completion;
 *                      an outcome that isn't listed is kept forever
 * @param breaker       when the topic stops claiming because most recent tasks fail
 * @param deadLetters   whether dead letters are revived on a new version or every so often
 */
record TopicSettings(
    int batchSize,
    int concurrency,
    Ordering ordering,
    Retry retry,
    Duration batchDeadline,
    Duration pollInterval,
    RateLimit rateLimit,
    Map<Outcome, Period> retention,
    Breaker breaker,
    DeadLetterPolicy deadLetters
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
        retention = Map.copyOf(retention);
        Objects.requireNonNull(breaker, "breaker");
        Objects.requireNonNull(deadLetters, "deadLetters");
    }

    /** 100 tasks per batch, 2 batches per node, FIFO, standard retry, 30 min deadline, 30 s poll. */
    public static TopicSettings defaults() {
        return new TopicSettings(100, 2, Ordering.FIFO, Retry.standard(), Duration.ofMinutes(30), Duration.ofSeconds(30), null, DEFAULT_RETENTION, Breaker.standard(),
            DeadLetterPolicy.KEEP);
    }

    public TopicSettings withBatchSize(int batchSize) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit, retention, breaker, deadLetters);
    }

    public TopicSettings withConcurrency(int concurrency) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit, retention, breaker, deadLetters);
    }

    public TopicSettings withOrdering(Ordering ordering) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit, retention, breaker, deadLetters);
    }

    public TopicSettings withRetry(Retry retry) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit, retention, breaker, deadLetters);
    }

    public TopicSettings withBatchDeadline(Duration batchDeadline) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit, retention, breaker, deadLetters);
    }

    public TopicSettings withPollInterval(Duration pollInterval) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit, retention, breaker, deadLetters);
    }

    public TopicSettings withRetention(Map<Outcome, Period> retention) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit, retention, breaker, deadLetters);
    }

    public TopicSettings withRateLimit(RateLimit rateLimit) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit, retention, breaker, deadLetters);
    }

    public TopicSettings withBreaker(Breaker breaker) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit, retention, breaker, deadLetters);
    }

    public TopicSettings withDeadLetters(DeadLetterPolicy deadLetters) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit, retention, breaker, deadLetters);
    }
}
