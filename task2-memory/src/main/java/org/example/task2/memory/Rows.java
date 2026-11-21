package org.example.task2.memory;

import java.time.Instant;
import java.time.Period;
import java.util.Map;

import org.example.task2.Outcome;

/** The in-memory tables. Rows are immutable, so a transaction can undo by putting the old row back. */
final class Rows {

    private Rows() {
    }

    enum State {
        SCHEDULED, RETRYING, READY, ACTIVE, SUSPENDED, FAILED;

        String label() {
            return name().toLowerCase();
        }
    }

    /** @param retention per outcome, or null if no node set it */
    record Topic(int id, String name, Instant createdAt, boolean paused, Map<Outcome, Period> retention) {

        Topic withRetention(Map<Outcome, Period> retention) {
            return new Topic(id, name, createdAt, paused, Map.copyOf(retention));
        }

        Topic withPaused(boolean paused) {
            return new Topic(id, name, createdAt, paused, retention);
        }
    }

    record Node(int id, String name, String version, Instant startedAt, Instant heartbeatAt) {

        Node beat(Instant now) {
            return new Node(id, name, version, startedAt, now);
        }
    }

    record Task(
        int topicId,
        long id,
        String identifier,
        String reference,
        String payload,
        String payloadType,
        Long parentId,
        Long rootId,
        Instant createdAt,
        String outcome,
        String previousOutcome,
        String output,
        String error,
        int attempts,
        Instant completedAt,
        String traceContext
    ) {

        Task finish(String outcome, String output, int attempts, Instant now) {
            return new Task(topicId, id, identifier, reference, payload, payloadType, parentId, rootId, createdAt,
                outcome, previousOutcome, output, error, attempts, now, traceContext);
        }

        Task redundant(Instant now) {
            return new Task(topicId, id, identifier, reference, payload, payloadType, parentId, rootId, createdAt,
                "redundant", previousOutcome, output, error, attempts, now, traceContext);
        }

        Task replaced() {
            return new Task(topicId, id, identifier, reference, payload, payloadType, parentId, rootId, createdAt,
                "redundant", outcome, output, error, attempts, completedAt, traceContext);
        }

        Task failed(String error, int attempts) {
            return new Task(topicId, id, identifier, reference, payload, payloadType, parentId, rootId, createdAt,
                outcome, previousOutcome, output, error, attempts, completedAt, traceContext);
        }
    }

    record Queued(
        long taskId,
        int topicId,
        String identifier,
        State state,
        int priority,
        Instant runAt,
        int attempt,
        Integer nodeId,
        Instant claimedAt,
        String dedupKey
    ) {

        Queued with(State state, Instant runAt, int attempt, Integer nodeId, Instant claimedAt) {
            return new Queued(taskId, topicId, identifier, state, priority, runAt, attempt, nodeId, claimedAt, dedupKey);
        }
    }

    record Attempt(int topicId, long taskId, int attempt, Integer nodeId, Instant startedAt, Instant finishedAt, String result, String error) {
    }

    record Bucket(String name, double tokens, Instant refilledAt) {
    }
}
