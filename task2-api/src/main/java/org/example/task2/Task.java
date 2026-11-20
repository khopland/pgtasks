package org.example.task2;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

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
 */
public record Task<T>(
    TopicRef<T> topic,
    long id,
    String identifier,
    T payload,
    int attempt,
    String reference,
    Long parentId,
    Long rootId,
    Instant createdAt,
    Instant claimedAt
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

    public OptionalLong findParentId() {
        return parentId == null ? OptionalLong.empty() : OptionalLong.of(parentId);
    }

    /** The root of this task's flow: its root, or the task itself if it has no parent. */
    public long flowRootId() {
        return rootId == null ? id : rootId;
    }
}
