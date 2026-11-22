package io.github.khopland.pgtasks;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
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
 * @param headers    names and values stored with the task, such as a correlation id. The
 *                   engine's observers add their own when the task is pushed, such as the
 *                   current trace's propagation headers; these win over theirs
 * @param runAt      if set, the task waits until this time before it becomes ready, by the
 *                   store's clock (the database's, on Postgres). A time that has come when the
 *                   push is written, the past included, makes it ready at once
 */
public record Creation<T>(
    String identifier,
    @Nullable T payload,
    @Nullable String reference,
    @Nullable Duration delay,
    boolean suspended,
    int priority,
    @Nullable String dedupKey,
    Map<String, String> headers,
    @Nullable Instant runAt
) {

    public Creation {
        Objects.requireNonNull(identifier, "identifier");
        if (identifier.isEmpty()) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        if (delay != null && delay.isNegative()) {
            throw new IllegalArgumentException("delay must not be negative");
        }
        if ((delay != null ? 1 : 0) + (runAt != null ? 1 : 0) + (suspended ? 1 : 0) > 1) {
            throw new IllegalArgumentException("a creation is either delayed, due at a time or suspended");
        }
        headers = Headers.copy(headers);
    }

    /** A creation without a run time, as before {@link #withRunAt} existed. */
    public Creation(String identifier, @Nullable T payload, @Nullable String reference, @Nullable Duration delay, boolean suspended, int priority,
                    @Nullable String dedupKey, Map<String, String> headers) {
        this(identifier, payload, reference, delay, suspended, priority, dedupKey, headers, null);
    }

    public static <T> Creation<T> of(String identifier, T payload) {
        return new Creation<>(identifier, payload, null, null, false, 0, null, Map.of(), null);
    }

    public Creation<T> withReference(String reference) {
        return new Creation<>(identifier, payload, reference, delay, suspended, priority, dedupKey, headers, runAt);
    }

    /** Waits this long from the push before the task becomes ready. Replaces a run time or a suspension. */
    public Creation<T> withDelay(Duration delay) {
        return new Creation<>(identifier, payload, reference, delay, false, priority, dedupKey, headers, null);
    }

    /**
     * Waits until this time, by the store's clock, before the task becomes ready: the absolute
     * form of {@link #withDelay}, for work due at a moment rather than after a while. A time that
     * has come when the push is written, the past included, makes the task ready at once. Replaces
     * a delay or a suspension.
     */
    public Creation<T> withRunAt(Instant runAt) {
        return new Creation<>(identifier, payload, reference, null, false, priority, dedupKey, headers, Objects.requireNonNull(runAt, "runAt"));
    }

    /** Waits until something releases the task. Replaces a delay or a run time. */
    public Creation<T> asSuspended() {
        return new Creation<>(identifier, payload, reference, null, true, priority, dedupKey, headers, null);
    }

    public Creation<T> withPriority(int priority) {
        return new Creation<>(identifier, payload, reference, delay, suspended, priority, dedupKey, headers, runAt);
    }

    public Creation<T> withDedupKey(String dedupKey) {
        return new Creation<>(identifier, payload, reference, delay, suspended, priority, dedupKey, headers, runAt);
    }

    /** Adds a header, or replaces the one with this name. */
    public Creation<T> withHeader(String name, String value) {
        return withHeaders(Map.of(name, value));
    }

    /** Adds these headers, replacing any with the same names. */
    public Creation<T> withHeaders(Map<String, String> headers) {
        Map<String, String> merged = new LinkedHashMap<>(this.headers);
        merged.putAll(Headers.copy(headers));
        return new Creation<>(identifier, payload, reference, delay, suspended, priority, dedupKey, merged, runAt);
    }
}
