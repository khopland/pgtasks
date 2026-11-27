package io.github.khopland.pgtasks.admin;

import java.time.Instant;

import org.jspecify.annotations.Nullable;

/**
 * Unfinished tasks of one topic. {@code due} counts delayed and retrying tasks whose time has
 * come; {@code nextRunAt} is the earliest time one of them is due, if any. {@code oldestReadyAt}
 * is when the oldest ready task (the lowest id) was pushed, if any are ready: how long the
 * topic's work has been waiting. A delayed task's delay counts in it. {@code failing} counts
 * unfinished tasks whose last run failed, dead letters included, and {@code oldestFailingAt}
 * is when the longest-failing of them first failed: the number to alert on.
 */
public record QueueCounts(long scheduled, long retrying, long ready, long active, long suspended, long deadLetters, long due,
                          @Nullable Instant nextRunAt, @Nullable Instant oldestReadyAt, long failing, @Nullable Instant oldestFailingAt) {

    public long total() {
        return scheduled + retrying + ready + active + suspended + deadLetters;
    }
}
