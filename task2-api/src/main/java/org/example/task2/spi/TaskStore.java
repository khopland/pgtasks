package org.example.task2.spi;

import org.example.task2.admin.AttemptView;
import org.example.task2.admin.DeadLetterQuery;
import org.example.task2.admin.FailureGroup;
import org.example.task2.admin.LimitOverride;
import org.example.task2.admin.QueueCounts;
import org.example.task2.admin.Schedule;
import org.example.task2.admin.TaskView;
import org.example.task2.admin.TopicSnapshot;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.time.Period;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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

    /** Every registered topic, by id. Nodes compare it with the topics they run on every heartbeat. */
    List<TopicInfo> topics(StoreTransaction transaction) throws SQLException;

    /**
     * Pauses or resumes a topic on every node: a paused topic still takes pushes, but no node
     * claims from it. Once committed, every node is notified.
     *
     * @return whether the topic exists
     */
    boolean setPaused(StoreTransaction transaction, String name, boolean paused) throws SQLException;

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

    /**
     * Makes up to {@code limit} due delayed tasks and retries ready, earliest first. Returns how
     * many it promoted, so a caller knows whether more are due, and the topics that got work.
     */
    Promoted promoteDue(StoreTransaction transaction, int limit) throws SQLException;

    /** @param tasks how many tasks became ready; @param topics the topics they belong to, each once */
    record Promoted(int tasks, List<Integer> topics) {
        public static final Promoted NONE = new Promoted(0, List.of());

        public Promoted {
            topics = List.copyOf(topics);
        }
    }

    /**
     * Requeues the active tasks of other nodes whose heartbeat is older than {@code expiry}.
     * The attempt is kept, so the expired run counts. Returns the affected topics.
     */
    List<Integer> requeueExpired(StoreTransaction transaction, int nodeId, Duration expiry, Expiries expiries) throws SQLException;

    /**
     * What an expiry does to a task, apart from its retry policy, so a task that takes its node
     * down can't do so forever. Each expiry counts as a failure and keeps the attempt.
     *
     * @param deadLetterAt the expiry that dead-letters the task, such as the third
     * @param backoff      how long the first expiry waits before the task is ready again, doubling
     *                     with each further one; zero makes it ready at once
     */
    record Expiries(int deadLetterAt, Duration backoff) {
        /** Ready at once, never dead-lettered: how expiry worked before it was counted. */
        public static final Expiries UNCOUNTED = new Expiries(Integer.MAX_VALUE, Duration.ZERO);

        public Expiries {
            Objects.requireNonNull(backoff, "backoff");
            if (deadLetterAt < 1) {
                throw new IllegalArgumentException("deadLetterAt must be at least 1");
            }
        }
    }

    /**
     * The tasks this node holds active. A node compares them with what it is actually processing,
     * to release claims it lost track of, such as one whose commit succeeded but whose reply never
     * arrived.
     */
    List<ActiveClaim> activeClaims(StoreTransaction transaction, int nodeId) throws SQLException;

    int registerNode(StoreTransaction transaction, String name, String version) throws SQLException;

    /**
     * Takes or renews this node's lease on a group of cluster-wide jobs, such as housekeeping. At
     * most one node holds a lease at a time. The holder keeps it by renewing it within its term;
     * once a term runs out, the lease goes to whichever node asks next, so a node that dies or
     * hangs loses it without closing anything.
     *
     * @return whether this node holds the lease for {@code term}, counted from the start of the
     *         transaction
     */
    boolean lead(StoreTransaction transaction, String name, int nodeId, Duration term) throws SQLException;

    /** Gives a lease up at once, if this node holds it. */
    void resign(StoreTransaction transaction, String name, int nodeId) throws SQLException;

    /**
     * Deletes up to {@code limit} finished tasks with this outcome that completed before the cutoff.
     * Unfinished tasks are never touched. Returns how many were deleted.
     */
    int purge(StoreTransaction transaction, int topicId, org.example.task2.Outcome outcome, Instant completedBefore, int limit) throws SQLException;

    /**
     * Records how long a topic's finished tasks are kept per outcome, so that whichever node leads
     * housekeeping applies it, also to topics that node doesn't run. Every node sets it when it
     * starts with the topic, so the last one to start wins; a topic nobody set it for keeps its
     * tasks forever. An outcome that isn't listed is kept forever.
     */
    void setRetention(StoreTransaction transaction, int topicId, Map<org.example.task2.Outcome, Period> retention) throws SQLException;

    /** The recorded retention of every topic that has one, by topic id. */
    Map<Integer, Map<org.example.task2.Outcome, Period>> retention(StoreTransaction transaction) throws SQLException;

    /** Records every topic's queue counts now. */
    void snapshotCounts(StoreTransaction transaction) throws SQLException;

    /** The last recorded queue counts per topic. */
    List<TopicSnapshot> snapshots(StoreTransaction transaction) throws SQLException;

    void heartbeat(StoreTransaction transaction, int nodeId) throws SQLException;

    /**
     * Deletes the rows of nodes whose last heartbeat is older than {@code deadFor} and that hold
     * no claims: every start adds a row, so rolling deploys and autoscaling would grow the table
     * forever. Returns how many.
     */
    int deleteDeadNodes(StoreTransaction transaction, Duration deadFor) throws SQLException;

    /** Makes sure the admission state for a strict rate limit exists. */
    void ensureAdmission(StoreTransaction transaction, String name, int burst) throws SQLException;

    /** Every rate limit override operators have set, reset ones included (plan section 08). */
    List<LimitOverride> limitOverrides(StoreTransaction transaction) throws SQLException;

    /**
     * Overrides a rate limit's rate, burst or both, optionally until a time; with both null, resets
     * it to the configured values. Every change bumps the override's version, which is 0 for a
     * limit never overridden, so it can serve as an ETag. Once committed, every node is notified.
     *
     * @param perSecond       the rate, or null to keep the configured one
     * @param burst           the burst, or null to keep the configured one
     * @param until           when the override lapses by itself, or null for never
     * @param by              who made the change, for the record
     * @param expectedVersion the version this change is based on, or null to change it regardless
     * @return the override as changed, or empty if its version isn't {@code expectedVersion}
     * @throws IllegalArgumentException if no node has configured the limit
     */
    Optional<LimitOverride> overrideLimit(StoreTransaction transaction, String name, Double perSecond, Integer burst, Instant until, String by,
                                          Long expectedVersion) throws SQLException;

    /** One task as tasks.task_view shows it. */
    Optional<TaskView> find(StoreTransaction transaction, long taskId) throws SQLException;

    /** The unfinished tasks of a topic, by queue state. */
    QueueCounts count(StoreTransaction transaction, int topicId) throws SQLException;

    /** A topic's dead letters that match the query, lowest id first, each with its last error. */
    List<TaskView> deadLetters(StoreTransaction transaction, int topicId, DeadLetterQuery query, int limit) throws SQLException;

    /** All of a topic's dead letters after an id: see {@link #deadLetters(StoreTransaction, int, DeadLetterQuery, int)}. */
    default List<TaskView> deadLetters(StoreTransaction transaction, int topicId, long afterId, int limit) throws SQLException {
        return deadLetters(transaction, topicId, DeadLetterQuery.ALL.after(afterId), limit);
    }

    /**
     * Gives the matching dead letters, the lowest ids first, another round of attempts: they become
     * ready, their attempts and failures back at zero, so the topic's whole retry policy applies
     * again. Their attempt log stays.
     *
     * @return the revived ids
     */
    List<Long> revive(StoreTransaction transaction, int topicId, DeadLetterQuery query, int limit) throws SQLException;

    /** Revives these dead letters, or the topic's oldest {@code limit} if null; others are skipped. */
    default List<Long> revive(StoreTransaction transaction, int topicId, Collection<Long> taskIds, int limit) throws SQLException {
        return revive(transaction, topicId, taskIds == null ? DeadLetterQuery.ALL : DeadLetterQuery.ids(taskIds), limit);
    }

    /**
     * Gives up on the matching dead letters: they leave the queue and finish as failed, kept as
     * long as the topic's retention for failed tasks says. Tasks they held back under an ordering
     * run next.
     *
     * @return the discarded ids
     */
    List<Long> discard(StoreTransaction transaction, int topicId, DeadLetterQuery query, int limit) throws SQLException;

    default List<Long> discard(StoreTransaction transaction, int topicId, Collection<Long> taskIds, int limit) throws SQLException {
        return discard(transaction, topicId, taskIds == null ? DeadLetterQuery.ALL : DeadLetterQuery.ids(taskIds), limit);
    }

    /**
     * Makes a topic's recurring tasks these: new ones are added, changed ones updated, the rest
     * removed. A schedule whose cron or zone is unchanged keeps its next run; one whose cron or
     * zone changed starts over from {@code nextRunAt}.
     */
    void syncSchedules(StoreTransaction transaction, int topicId, List<ScheduleSpec> schedules) throws SQLException;

    /** Every topic's recurring tasks, by topic and name. */
    List<Schedule> schedules(StoreTransaction transaction) throws SQLException;

    /**
     * Recurring tasks due by the store's clock, earliest first, locked until the transaction ends,
     * so that two nodes never fire one twice.
     */
    List<Schedule> dueSchedules(StoreTransaction transaction, int limit) throws SQLException;

    /** Records that a recurring task fired, and when it fires next. */
    void scheduleFired(StoreTransaction transaction, int scheduleId, Instant firedAt, Instant nextRunAt) throws SQLException;

    /**
     * A topic's failing tasks grouped by error type (or, for errors without one, by message), the
     * largest group first, with when the longest-failing task of each first failed.
     */
    List<FailureGroup> failing(StoreTransaction transaction, int topicId, int limit) throws SQLException;

    /**
     * Records that the topic's dead letters were revived for this application version. True for
     * the first node of the version to ask, false for every other: they are revived once.
     */
    boolean claimRevival(StoreTransaction transaction, int topicId, String version) throws SQLException;

    /** Claims a sweep of the topic's dead letters: true at most once per {@code every}, across the cluster. */
    boolean claimReviveSweep(StoreTransaction transaction, int topicId, Duration every) throws SQLException;

    /** The logged attempts of a task: failed and expired ones. */
    List<AttemptView> attempts(StoreTransaction transaction, long taskId) throws SQLException;

    /**
     * A recurring task as configured: what to push, when, and how. The payload is encoded; the
     * cron is read in {@code zone}.
     */
    record ScheduleSpec(String name, String identifier, String cron, String zone, Insertion insertion, String payload, String payloadType,
                        Instant nextRunAt) {
        public ScheduleSpec {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(identifier, "identifier");
            Objects.requireNonNull(cron, "cron");
            Objects.requireNonNull(zone, "zone");
            Objects.requireNonNull(insertion, "insertion");
            Objects.requireNonNull(nextRunAt, "nextRunAt");
        }
    }

    /**
     * A task to store. Payload and type are already encoded. {@code traceContext} is the pushing
     * code's trace, as a W3C {@code traceparent}, or null.
     */
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
        Long rootId,
        String traceContext
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

    record TopicInfo(int id, String name, boolean paused) {
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
        Instant claimedAt,
        String traceContext,
        int failures,
        Instant firstFailedAt
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

    record ActiveClaim(int topicId, long taskId, int attempt, Instant claimedAt) {
    }

}
