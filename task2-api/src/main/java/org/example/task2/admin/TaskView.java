package org.example.task2.admin;

import java.time.Instant;

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
    Instant runAt,
    int priority,
    Integer nodeId,
    String reference,
    Long parentId,
    Long rootId,
    String payload,
    String payloadType,
    String output,
    String error,
    String previousOutcome,
    Instant createdAt,
    Instant completedAt,
    int failures,
    Instant firstFailedAt
) {
}
