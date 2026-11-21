package io.github.khopland.pgtasks;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import org.jspecify.annotations.Nullable;

/**
 * A claimed task, as a handler sees it.
 *
 * @param topic     the topic the task belongs to
 * @param id        the task's sequence number, unique across topics
 * @param identifier the application key used for insertion policies and ordering
 * @param payload   the decoded payload, or {@code null} if the task has none
 * @param attempt   the counted attempt this claim belongs to, starting at 1. Suspended, delayed
 *                  and throttled runs don't count
 * @param reference an optional correlation value
 * @param parentId  the task that pushed this one, if any
 * @param rootId    the first task of the flow this one belongs to, if it has a parent
 * @param createdAt when the task was pushed
 * @param claimedAt when this attempt was claimed
 * @param traceContext the trace of the code that pushed the task, as a W3C {@code traceparent},
 *                  or {@code null} if none was recorded
 */
public record Task<T>(
    TopicRef<T> topic,
    long id,
    String identifier,
    @Nullable T payload,
    int attempt,
    @Nullable String reference,
    @Nullable Long parentId,
    @Nullable Long rootId,
    Instant createdAt,
    Instant claimedAt,
    @Nullable String traceContext
) {

    public Task {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(identifier, "identifier");
    }

    /**
     * A key that stays the same across automatic retries of this task, for deduplicating
     * external effects. An explicit recreate creates a new task and so a new key.
     */
    public String idempotencyKey() {
        return topic.name() + "/" + id;
    }

    public Optional<String> findReference() {
        return Optional.ofNullable(reference);
    }

    public Optional<String> findTraceContext() {
        return Optional.ofNullable(traceContext);
    }

    public OptionalLong findParentId() {
        return parentId == null ? OptionalLong.empty() : OptionalLong.of(parentId);
    }

    /** The root of this task's flow: its root, or the task itself if it has no parent. */
    public long flowRootId() {
        return rootId == null ? id : rootId;
    }
}
