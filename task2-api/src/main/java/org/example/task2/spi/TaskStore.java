package org.example.task2.spi;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

import org.example.task2.Insertion;
import org.example.task2.Ordering;
import org.example.task2.Pushed;

/**
 * Storage for tasks. Every method works inside the caller's transaction on the given
 * connection and never commits.
 */
public interface TaskStore {

    /** Registers a topic and creates its storage, if it doesn't exist yet. Returns its id. */
    int createTopic(Connection connection, String name) throws SQLException;

    OptionalInt findTopic(Connection connection, String name) throws SQLException;

    /** Pushes all tasks in one statement, in order, applying the insertion policy. */
    List<Pushed> push(Connection connection, int topicId, Insertion insertion, List<NewTask> tasks) throws SQLException;

    /**
     * Claims up to {@code request.limit()} ready tasks, honouring the ordering and, if set, the
     * strict rate admission. Each claim increments the task's attempt.
     */
    Claim claim(Connection connection, ClaimRequest request) throws SQLException;

    /**
     * Applies the completions, fenced: a completion only matches a task that is still active
     * under this node with the same attempt. If any doesn't match, throws
     * {@link StaleClaimException} and the caller must roll back.
     */
    void complete(Connection connection, int topicId, int nodeId, List<Completion> completions) throws SQLException;

    /**
     * Like {@link #complete}, but applies whatever still matches and skips the rest. For
     * recording failures after a completion transaction rolled back. Returns how many matched.
     */
    int release(Connection connection, int topicId, int nodeId, List<Completion> completions) throws SQLException;

    /** Makes due delayed tasks and retries ready. Returns the topics that got ready work. */
    List<Integer> promoteDue(Connection connection, int limit) throws SQLException;

    /**
     * Requeues the active tasks of other nodes whose heartbeat is older than {@code expiry}.
     * The attempt is kept, so the expired run counts. Returns the affected topics.
     */
    List<Integer> requeueExpired(Connection connection, int nodeId, Duration expiry) throws SQLException;

    int registerNode(Connection connection, String name, String version) throws SQLException;

    void heartbeat(Connection connection, int nodeId) throws SQLException;

    /** Makes sure the admission state for a strict rate limit exists. */
    void ensureAdmission(Connection connection, String name, int burst) throws SQLException;

    /** A task to store. Payload and type are already encoded. */
    record NewTask(
        String identifier,
        String payload,
        String payloadType,
        String reference,
        Duration delay,
        boolean suspended,
        int priority,
        String dedupKey,
        Long parentId,
        Long rootId
    ) {
        public NewTask {
            Objects.requireNonNull(identifier, "identifier");
        }
    }

    /**
     * A strict, cluster-wide rate limit: a token bucket kept in the database. Over any interval
     * of length t, at most {@code burst + perSecond * t} tasks are claimed, across every node.
     */
    record Admission(String name, double perSecond, int burst) {
        public Admission {
            Objects.requireNonNull(name, "name");
            if (!(perSecond > 0)) {
                throw new IllegalArgumentException("rate must be positive; pausing is a separate switch");
            }
            if (burst < 1) {
                throw new IllegalArgumentException("burst must be at least 1");
            }
        }
    }

    record ClaimRequest(int topicId, int nodeId, Ordering ordering, int limit, Admission admission) {
        public ClaimRequest {
            Objects.requireNonNull(ordering, "ordering");
            if (limit < 1) {
                throw new IllegalArgumentException("limit must be at least 1");
            }
        }
    }

    record ClaimedTask(
        long id,
        String identifier,
        int attempt,
        int priority,
        String payload,
        String payloadType,
        String reference,
        Long parentId,
        Long rootId,
        Instant createdAt,
        Instant claimedAt
    ) {
    }

    /**
     * @param tasks        the claimed tasks
     * @param admissionWait if the rate limit allowed nothing, how long until it allows one task
     */
    record Claim(List<ClaimedTask> tasks, Duration admissionWait) {
        public static final Claim EMPTY = new Claim(List.of(), null);
    }

    record Completion(long taskId, int attempt, Instant claimedAt, Resolution resolution) {
        public Completion {
            Objects.requireNonNull(resolution, "resolution");
        }
    }

    sealed interface Resolution {

        /** The task leaves the queue with this outcome. */
        record Finished(Outcome outcome, String output) implements Resolution {
        }

        /**
         * The task stays in the queue, waiting.
         *
         * @param state   what it waits in
         * @param delay   for {@code SCHEDULED} and {@code RETRYING}: until it becomes ready
         * @param counted whether this attempt counts against the retry policy
         * @param error   the failure, as a JSON object, for failed attempts
         */
        record Waiting(WaitState state, Duration delay, boolean counted, String error) implements Resolution {
            public Waiting {
                Objects.requireNonNull(state, "state");
                if ((state == WaitState.SCHEDULED || state == WaitState.RETRYING) != (delay != null)) {
                    throw new IllegalArgumentException(state + " needs a delay exactly when it waits for a time");
                }
            }
        }
    }

    enum Outcome {
        SUCCEEDED, FILTERED
    }

    enum WaitState {
        /** Delayed; runs at a time, doesn't hold back later tasks under SINGULAR ordering. */
        SCHEDULED,
        /** Backing off after a failure or throttling; holds its place in line. */
        RETRYING,
        /** Waits until released. */
        SUSPENDED,
        /** Out of attempts; waits for an operator to revive or discard it. */
        DEAD_LETTER
    }
}
