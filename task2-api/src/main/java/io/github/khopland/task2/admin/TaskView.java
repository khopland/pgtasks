package io.github.khopland.task2.admin;

import java.time.Instant;

import org.jspecify.annotations.Nullable;

/**
 * A task, unfinished or not.
 *
 * @param state           the queue state if unfinished (scheduled, retrying, ready, active,
 *                        suspended, failed), otherwise the outcome (succeeded, filtered,
 *                        failed, redundant, recreated)
 * @param attempt         the current attempt if unfinished, otherwise the attempts it took
 * @param error           the last recorded failure as JSON, kept from the first failure on
 * @param failures        failed runs in a row while unfinished, counted or not
 * @param firstFailedAt   when the first of those failed, or null
 */
public record TaskView(
    String topic,
    long id,
    String identifier,
    String state,
    boolean finished,
    int attempt,
    @Nullable Instant runAt,
    int priority,
    @Nullable Integer nodeId,
    @Nullable String reference,
    @Nullable Long parentId,
    @Nullable Long rootId,
    @Nullable String payload,
    @Nullable String payloadType,
    @Nullable String output,
    @Nullable String error,
    @Nullable String previousOutcome,
    Instant createdAt,
    @Nullable Instant completedAt,
    int failures,
    @Nullable Instant firstFailedAt
) {
}
