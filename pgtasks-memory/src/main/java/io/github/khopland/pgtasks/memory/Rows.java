package io.github.khopland.pgtasks.memory;

import java.time.Instant;
import java.time.Period;
import java.util.Locale;
import java.util.Map;

import io.github.khopland.pgtasks.Outcome;

import org.jspecify.annotations.Nullable;

/** The in-memory tables. Rows are immutable, so a transaction can undo by putting the old row back. */
final class Rows {

    private Rows() {
    }

    enum State {
        SCHEDULED, RETRYING, READY, ACTIVE, SUSPENDED, FAILED;

        String label() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /** @param retention per outcome, or null if no node set it */
    record Topic(int id, String name, Instant createdAt, boolean paused, @Nullable Map<Outcome, Period> retention,
                 @Nullable String revivedVersion, @Nullable Instant reviveSweptAt) {

        Topic(int id, String name, Instant createdAt, boolean paused, @Nullable Map<Outcome, Period> retention) {
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

    record Node(int id, String name, @Nullable String version, Instant startedAt, Instant heartbeatAt) {

        Node beat(Instant now) {
            return new Node(id, name, version, startedAt, now);
        }
    }

    record Task(
        int topicId,
        long id,
        String identifier,
        @Nullable String reference,
        @Nullable String payload,
        @Nullable String payloadType,
        @Nullable Long parentId,
        @Nullable Long rootId,
        Instant createdAt,
        @Nullable String outcome,
        @Nullable String previousOutcome,
        @Nullable String output,
        @Nullable String error,
        int attempts,
        @Nullable Instant completedAt,
        Map<String, String> headers
    ) {

        Task finish(String outcome, @Nullable String output, int attempts, Instant now) {
            return new Task(topicId, id, identifier, reference, payload, payloadType, parentId, rootId, createdAt,
                outcome, previousOutcome, output, error, attempts, now, headers);
        }

        Task redundant(Instant now) {
            return new Task(topicId, id, identifier, reference, payload, payloadType, parentId, rootId, createdAt,
                "redundant", previousOutcome, output, error, attempts, now, headers);
        }

        Task replaced() {
            return new Task(topicId, id, identifier, reference, payload, payloadType, parentId, rootId, createdAt,
                "redundant", outcome, output, error, attempts, completedAt, headers);
        }

        Task failed(String error, int attempts) {
            return new Task(topicId, id, identifier, reference, payload, payloadType, parentId, rootId, createdAt,
                outcome, previousOutcome, output, error, attempts, completedAt, headers);
        }
    }

    record Schedule(int id, int topicId, String name, String identifier, String cron, String zone, String insertion,
                    @Nullable String payload, @Nullable String payloadType, Instant nextRunAt, @Nullable Instant lastRunAt) {

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
        @Nullable Instant runAt,
        int attempt,
        @Nullable Integer nodeId,
        @Nullable Instant claimedAt,
        @Nullable String dedupKey,
        int failures,
        @Nullable Instant firstFailedAt,
        int expiries
    ) {

        Queued with(State state, @Nullable Instant runAt, int attempt, @Nullable Integer nodeId, @Nullable Instant claimedAt) {
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

    record Attempt(int topicId, long taskId, int attempt, @Nullable Integer nodeId, Instant startedAt, Instant finishedAt, String result,
                   @Nullable String error) {
    }
}
