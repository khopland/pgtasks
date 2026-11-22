package io.github.khopland.pgtasks.admin;

import java.time.Instant;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.jspecify.annotations.Nullable;

/**
 * Which unfinished tasks of a topic: some by id or all, after an id (for paging, and so that
 * acting in batches never meets a task twice), narrowed by identifier, reference, queue state,
 * one header's value and when they were pushed. Every condition that is set must hold; null, or
 * no states, means any.
 *
 * @param taskIds     these tasks only, or null for all
 * @param afterId     only tasks with a higher id; 0 for the first page
 * @param identifier  the exact identifier
 * @param reference   the exact reference
 * @param states      any of these states; empty for any state
 * @param headerName  a header the task was pushed with, which must have {@code headerValue}
 * @param headerValue the header's value; set exactly when {@code headerName} is
 * @param createdFrom pushed at or after this time
 * @param createdUntil pushed before this time
 */
public record TaskQuery(@Nullable Collection<Long> taskIds, long afterId, @Nullable String identifier, @Nullable String reference,
                        Set<QueueState> states, @Nullable String headerName, @Nullable String headerValue,
                        @Nullable Instant createdFrom, @Nullable Instant createdUntil) {

    /** Every unfinished task of the topic. */
    public static final TaskQuery ALL = new TaskQuery(null, 0, null, null, Set.of(), null, null, null, null);

    public TaskQuery {
        taskIds = taskIds == null ? null : List.copyOf(taskIds);
        states = states.isEmpty() ? Set.of() : Set.copyOf(EnumSet.copyOf(states));
        if ((headerName == null) != (headerValue == null)) {
            throw new IllegalArgumentException("A header condition needs both a name and a value");
        }
        if (headerName != null && headerName.isEmpty()) {
            throw new IllegalArgumentException("A header name must not be empty");
        }
    }

    public static TaskQuery ids(Collection<Long> taskIds) {
        return ALL.withIds(Objects.requireNonNull(taskIds, "taskIds"));
    }

    public TaskQuery withIds(@Nullable Collection<Long> taskIds) {
        return new TaskQuery(taskIds, afterId, identifier, reference, states, headerName, headerValue, createdFrom, createdUntil);
    }

    public TaskQuery after(long id) {
        return new TaskQuery(taskIds, id, identifier, reference, states, headerName, headerValue, createdFrom, createdUntil);
    }

    public TaskQuery withIdentifier(@Nullable String identifier) {
        return new TaskQuery(taskIds, afterId, identifier, reference, states, headerName, headerValue, createdFrom, createdUntil);
    }

    public TaskQuery withReference(@Nullable String reference) {
        return new TaskQuery(taskIds, afterId, identifier, reference, states, headerName, headerValue, createdFrom, createdUntil);
    }

    /** Tasks in any of these states; none for any state. */
    public TaskQuery inStates(Collection<QueueState> states) {
        return new TaskQuery(taskIds, afterId, identifier, reference, Set.copyOf(states), headerName, headerValue, createdFrom, createdUntil);
    }

    public TaskQuery inStates(QueueState... states) {
        return inStates(List.of(states));
    }

    /** Tasks pushed with this header and value; nulls for any. */
    public TaskQuery withHeader(@Nullable String name, @Nullable String value) {
        return new TaskQuery(taskIds, afterId, identifier, reference, states, name, value, createdFrom, createdUntil);
    }

    /** Tasks pushed in this time range: from inclusive, until exclusive, either null for open. */
    public TaskQuery createdBetween(@Nullable Instant from, @Nullable Instant until) {
        return new TaskQuery(taskIds, afterId, identifier, reference, states, headerName, headerValue, from, until);
    }

    /** Whether anything narrows the query besides the ids and the paging. */
    public boolean filtered() {
        return identifier != null || reference != null || !states.isEmpty() || headerName != null || createdFrom != null || createdUntil != null;
    }

    /** Whether a task in this state, with these values, matches the query, ids and paging included. */
    public boolean matches(long id, String identifier, @Nullable String reference, QueueState state, Map<String, String> headers,
                           Instant createdAt) {
        return id > afterId
            && (taskIds == null || taskIds.contains(id))
            && (this.identifier == null || this.identifier.equals(identifier))
            && (this.reference == null || this.reference.equals(reference))
            && (states.isEmpty() || states.contains(state))
            && (headerName == null || Objects.equals(headers.get(headerName), headerValue))
            && (createdFrom == null || !createdAt.isBefore(createdFrom))
            && (createdUntil == null || createdAt.isBefore(createdUntil));
    }
}
