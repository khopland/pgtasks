package io.github.khopland.pgtasks.admin;

import java.time.Instant;
import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * A task, unfinished or not.
 *
 * @param state           the queue state if unfinished (scheduled, retrying, ready, active,
 *                        suspended, dead_letter), otherwise the outcome (succeeded, filtered,
 *                        failed, redundant, recreated, cancelled)
 * @param attempt         the current attempt if unfinished, otherwise the attempts it took
 * @param error           the last recorded failure as JSON, kept from the first failure on
 * @param failures        failed runs in a row while unfinished, counted or not
 * @param firstFailedAt   when the first of those failed, or null
 * @param headers         the headers the task was pushed with
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
    @Nullable Instant firstFailedAt,
    Map<String, String> headers
) {

    public TaskView {
        headers = Map.copyOf(headers);
    }
}
