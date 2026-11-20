package org.example.task2.spi;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import org.example.task2.Insertion;
import org.example.task2.Juncture;
import org.example.task2.Ordering;
import org.example.task2.Pushed;

/**
 * Storage for tasks. Every operation runs in a {@link StoreTransaction} and never commits it.
 * Two implementations pass the same TCK: Postgres, and the in-memory store for local runs and
 * fast tests.
 */
public interface TaskStore {

    /** Called once when an engine starts, before anything else: check, or migrate, the storage. */
    default void prepare() throws SQLException {
    }

    /**
     * The clock the store stamps tasks with. Postgres uses database time; the in-memory store
     * takes an injectable clock, which the engine then uses too.
     */
    default InstantSource clock() {
        return InstantSource.system();
    }

    /** Starts a transaction for the engine's own work. */
    StoreTransaction begin() throws SQLException;

    /**
     * Whether pushes can join the application's JDBC transactions, because the tasks live in
     * the application's database. If not, pushes made in an application transaction are held
     * back until it commits.
     */
    default boolean joinsApplicationTransactions() {
        return false;
    }

    /** Wraps an application's JDBC transaction; only for stores that join them. */
    default StoreTransaction join(Connection connection) {
        throw new UnsupportedOperationException(this + " keeps its tasks outside the application's database and can't join its transactions");
    }

    /** Registers a topic and creates its storage, if it doesn't exist yet. Returns its id. */
    int createTopic(StoreTransaction transaction, String name) throws SQLException;

    OptionalInt findTopic(StoreTransaction transaction, String name) throws SQLException;

    /** Pushes all tasks in one step, in order, applying the insertion policy. */
    List<Pushed> push(StoreTransaction transaction, int topicId, Insertion insertion, List<NewTask> tasks) throws SQLException;

    /**
     * For each group of identifiers: if every identifier has a task in the topic that counts for
     * the juncture, the group's suspended tasks become ready. Returns the released identifiers.
     */
    Set<String> junction(StoreTransaction transaction, int topicId, Juncture juncture, Collection<Set<String>> groups) throws SQLException;

    /**
     * Claims up to {@code request.limit()} ready tasks, honouring the ordering and, if set, the
     * strict rate admission. Each claim increments the task's attempt.
     */
    Claim claim(StoreTransaction transaction, ClaimRequest request) throws SQLException;

    /**
     * Applies the completions, fenced: a completion only matches a task that is still active
     * under this node with the same attempt. If any doesn't match, throws
     * {@link StaleClaimException} and the caller must roll back.
     */
    void complete(StoreTransaction transaction, int topicId, int nodeId, List<Completion> completions) throws SQLException;

    /**
     * Like {@link #complete}, but applies whatever still matches and skips the rest. For
     * recording failures after a completion transaction rolled back. Returns how many matched.
     */
    int release(StoreTransaction transaction, int topicId, int nodeId, List<Completion> completions) throws SQLException;

    /** Makes due delayed tasks and retries ready. Returns the topics that got ready work. */
    List<Integer> promoteDue(StoreTransaction transaction, int limit) throws SQLException;

    /**
     * Requeues the active tasks of other nodes whose heartbeat is older than {@code expiry}.
     * The attempt is kept, so the expired run counts. Returns the affected topics.
     */
    List<Integer> requeueExpired(StoreTransaction transaction, int nodeId, Duration expiry) throws SQLException;

    /**
     * The tasks this node holds active. A node compares them with what it is actually processing,
     * to release claims it lost track of, such as one whose commit succeeded but whose reply never
     * arrived.
     */
    List<ActiveClaim> activeClaims(StoreTransaction transaction, int nodeId) throws SQLException;

    int registerNode(StoreTransaction transaction, String name, String version) throws SQLException;

    void heartbeat(StoreTransaction transaction, int nodeId) throws SQLException;

    /** Makes sure the admission state for a strict rate limit exists. */
    void ensureAdmission(StoreTransaction transaction, String name, int burst) throws SQLException;

    /** One task as tasks.task_view shows it. */
    Optional<TaskView> find(StoreTransaction transaction, long taskId) throws SQLException;

    /** The unfinished tasks of a topic, by queue state. */
    QueueCounts count(StoreTransaction transaction, int topicId) throws SQLException;

    /** The logged attempts of a task: failed and expired ones. */
    List<AttemptView> attempts(StoreTransaction transaction, long taskId) throws SQLException;

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
     * A strict, cluster-wide rate limit: a token bucket kept in the store. Over any interval of
     * length t, at most {@code burst + perSecond * t} tasks are claimed, across every node.
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
     * @param tasks         the claimed tasks
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

    /**
     * A task, unfinished or not.
     *
     * @param state           the queue state if unfinished (scheduled, retrying, ready, active,
     *                        suspended, failed), otherwise the outcome (succeeded, filtered,
     *                        failed, redundant, recreated)
     * @param attempt         the current attempt if unfinished, otherwise the attempts it took
     * @param error           the last recorded failure as JSON, for dead letters
     */
    record TaskView(
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
        Instant completedAt
    ) {
    }

    /**
     * Unfinished tasks of one topic. {@code due} counts delayed and retrying tasks whose time has
     * come; {@code nextRunAt} is the earliest time one of them is due, if any.
     */
    record QueueCounts(long scheduled, long retrying, long ready, long active, long suspended, long failed, long due, Instant nextRunAt) {

        public long total() {
            return scheduled + retrying + ready + active + suspended + failed;
        }
    }

    record ActiveClaim(int topicId, long taskId, int attempt, Instant claimedAt) {
    }

    /** @param result failed or expired */
    record AttemptView(long taskId, int attempt, Integer nodeId, String result, String error) {
    }
}
