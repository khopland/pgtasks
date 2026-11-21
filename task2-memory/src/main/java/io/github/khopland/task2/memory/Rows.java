package io.github.khopland.task2.memory;

import java.time.Instant;
import java.time.Period;
import java.util.Map;

import io.github.khopland.task2.Outcome;

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
    record Topic(int id, String name, Instant createdAt, boolean paused, Map<Outcome, Period> retention, String revivedVersion,
                 Instant reviveSweptAt) {

        Topic(int id, String name, Instant createdAt, boolean paused, Map<Outcome, Period> retention) {
            this(id, name, createdAt, paused, retention, null, null);
        }

        Topic withRetention(Map<Outcome, Period> retention) {
            return new Topic(id, name, createdAt, paused, Map.copyOf(retention), revivedVersion, reviveSweptAt);
        }

        Topic withPaused(boolean paused) {
            return new Topic(id, name, createdAt, paused, retention, revivedVersion, reviveSweptAt);
        }

        Topic revivedFor(String version) {
            return new Topic(id, name, createdAt, paused, retention, version, reviveSweptAt);
        }

        Topic sweptAt(Instant now) {
            return new Topic(id, name, createdAt, paused, retention, revivedVersion, now);
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

    record Schedule(int id, int topicId, String name, String identifier, String cron, String zone, String insertion,
                    String payload, String payloadType, Instant nextRunAt, Instant lastRunAt) {

        Schedule fired(Instant at, Instant next) {
            return new Schedule(id, topicId, name, identifier, cron, zone, insertion, payload, payloadType, next, at);
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
        String dedupKey,
        int failures,
        Instant firstFailedAt,
        int expiries
    ) {

        Queued with(State state, Instant runAt, int attempt, Integer nodeId, Instant claimedAt) {
            return new Queued(taskId, topicId, identifier, state, priority, runAt, attempt, nodeId, claimedAt, dedupKey, failures, firstFailedAt, expiries);
        }

        /** One more failed run, counted or not. */
        Queued failedAt(Instant now) {
            return new Queued(taskId, topicId, identifier, state, priority, runAt, attempt, nodeId, claimedAt, dedupKey, failures + 1,
                firstFailedAt == null ? now : firstFailedAt, expiries);
        }

        /** Its node died under it: a failure, and one more expiry. */
        Queued expiredAt(Instant now) {
            Queued failed = failedAt(now);
            return new Queued(taskId, topicId, identifier, failed.state, priority, failed.runAt, attempt, failed.nodeId, failed.claimedAt, dedupKey,
                failed.failures, failed.firstFailedAt, expiries + 1);
        }

        Queued withoutFailures() {
            return new Queued(taskId, topicId, identifier, state, priority, runAt, attempt, nodeId, claimedAt, dedupKey, 0, null, 0);
        }
    }

    record Attempt(int topicId, long taskId, int attempt, Integer nodeId, Instant startedAt, Instant finishedAt, String result, String error) {
    }

    record Bucket(String name, double tokens, Instant refilledAt) {
    }
}
