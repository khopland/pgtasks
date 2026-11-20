package org.example.task2.engine;

import java.time.Duration;
import java.util.Objects;

import org.example.task2.Ordering;

/**
 * Per-topic settings. {@link #defaults()} is safe in production; change only what differs.
 *
 * @param batchSize     most tasks claimed per batch
 * @param concurrency   batches in flight per topic on one node
 * @param ordering      per-identifier ordering
 * @param retry         what happens to failed attempts
 * @param batchDeadline a batch whose handler runs longer is cancelled and its tasks retried
 * @param pollInterval  fallback poll when no notification arrives
 * @param rateLimit     optional strict admission limit
 */
public record TopicSettings(
    int batchSize,
    int concurrency,
    Ordering ordering,
    Retry retry,
    Duration batchDeadline,
    Duration pollInterval,
    RateLimit rateLimit
) {

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
    }

    /** 100 tasks per batch, 2 batches per node, FIFO, standard retry, 30 min deadline, 30 s poll. */
    public static TopicSettings defaults() {
        return new TopicSettings(100, 2, Ordering.FIFO, Retry.standard(), Duration.ofMinutes(30), Duration.ofSeconds(30), null);
    }

    public TopicSettings withBatchSize(int batchSize) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit);
    }

    public TopicSettings withConcurrency(int concurrency) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit);
    }

    public TopicSettings withOrdering(Ordering ordering) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit);
    }

    public TopicSettings withRetry(Retry retry) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit);
    }

    public TopicSettings withBatchDeadline(Duration batchDeadline) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit);
    }

    public TopicSettings withPollInterval(Duration pollInterval) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit);
    }

    public TopicSettings withRateLimit(RateLimit rateLimit) {
        return new TopicSettings(batchSize, concurrency, ordering, retry, batchDeadline, pollInterval, rateLimit);
    }
}
