package org.example.task2.admin;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Which dead letters: some by id or all, after an id (for paging, and so that reviving in
 * batches never meets a task twice), with an error (its type, or its message if it has none,
 * as the failing groups do), first failed in a time range. Null means any.
 */
public record DeadLetterQuery(Collection<Long> taskIds, long afterId, String error, Instant failedFrom, Instant failedUntil) {

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

    public DeadLetterQuery withError(String error) {
        return new DeadLetterQuery(taskIds, afterId, error, failedFrom, failedUntil);
    }

    public DeadLetterQuery failedBetween(Instant from, Instant until) {
        return new DeadLetterQuery(taskIds, afterId, error, from, until);
    }
}
