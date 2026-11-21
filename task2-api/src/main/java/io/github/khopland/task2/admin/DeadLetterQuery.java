package io.github.khopland.task2.admin;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * Which dead letters: some by id or all, after an id (for paging, and so that reviving in
 * batches never meets a task twice), with an error (its type, or its message if it has none,
 * as the failing groups do), first failed in a time range. Null means any.
 */
public record DeadLetterQuery(@Nullable Collection<Long> taskIds, long afterId, @Nullable String error, @Nullable Instant failedFrom,
                              @Nullable Instant failedUntil) {

    public static final DeadLetterQuery ALL = new DeadLetterQuery(null, 0, null, null, null);

    public DeadLetterQuery {
        taskIds = taskIds == null ? null : List.copyOf(taskIds);
    }

    public static DeadLetterQuery ids(Collection<Long> taskIds) {
        return new DeadLetterQuery(Objects.requireNonNull(taskIds, "taskIds"), 0, null, null, null);
    }

    public DeadLetterQuery after(long id) {
        return new DeadLetterQuery(taskIds, id, error, failedFrom, failedUntil);
    }

    public DeadLetterQuery withError(@Nullable String error) {
        return new DeadLetterQuery(taskIds, afterId, error, failedFrom, failedUntil);
    }

    public DeadLetterQuery failedBetween(@Nullable Instant from, @Nullable Instant until) {
        return new DeadLetterQuery(taskIds, afterId, error, from, until);
    }
}
