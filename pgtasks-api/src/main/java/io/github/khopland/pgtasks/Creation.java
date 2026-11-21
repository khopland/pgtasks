package io.github.khopland.pgtasks;

import java.time.Duration;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * A task to push.
 *
 * @param identifier the application key
 * @param payload    the payload, encoded with the topic's codec
 * @param reference  an optional correlation value
 * @param delay      if set, the task waits this long before it becomes ready
 * @param suspended  if set, the task waits until something releases it
 * @param priority   higher runs first; 0 by default, which keeps plain FIFO order
 * @param dedupKey   if set, at most one unfinished task per key and topic exists; a push with a
 *                   key that is already queued is reported as a duplicate and not stored
 */
public record Creation<T>(
    String identifier,
    @Nullable T payload,
    @Nullable String reference,
    @Nullable Duration delay,
    boolean suspended,
    int priority,
    @Nullable String dedupKey
) {

    public Creation {
        Objects.requireNonNull(identifier, "identifier");
        if (identifier.isEmpty()) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        if (delay != null && delay.isNegative()) {
            throw new IllegalArgumentException("delay must not be negative");
        }
        if (delay != null && suspended) {
            throw new IllegalArgumentException("a creation is either delayed or suspended");
        }
    }

    public static <T> Creation<T> of(String identifier, T payload) {
        return new Creation<>(identifier, payload, null, null, false, 0, null);
    }

    public Creation<T> withReference(String reference) {
        return new Creation<>(identifier, payload, reference, delay, suspended, priority, dedupKey);
    }

    public Creation<T> withDelay(Duration delay) {
        return new Creation<>(identifier, payload, reference, delay, false, priority, dedupKey);
    }

    public Creation<T> asSuspended() {
        return new Creation<>(identifier, payload, reference, null, true, priority, dedupKey);
    }

    public Creation<T> withPriority(int priority) {
        return new Creation<>(identifier, payload, reference, delay, suspended, priority, dedupKey);
    }

    public Creation<T> withDedupKey(String dedupKey) {
        return new Creation<>(identifier, payload, reference, delay, suspended, priority, dedupKey);
    }
}
