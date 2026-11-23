package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.github.khopland.pgtasks.Outcome;
import io.github.khopland.pgtasks.TaskStorageException;
import io.github.khopland.pgtasks.admin.AttemptView;
import io.github.khopland.pgtasks.admin.DeadLetterQuery;
import io.github.khopland.pgtasks.admin.FailureGroup;
import io.github.khopland.pgtasks.admin.NodeView;
import io.github.khopland.pgtasks.admin.QueueState;
import io.github.khopland.pgtasks.admin.TaskQuery;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.admin.TopicSnapshot;
import io.github.khopland.pgtasks.admin.TopicStatus;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The operator's side of an engine, from {@link TaskEngine#admin()}: topics, tasks (searched,
 * cancelled, run now), dead letters and retention. Everything here reads or changes the store directly, so it works on a node that
 * only pushes, and acts on the whole cluster.
 * Failures are unchecked: {@link TaskStorageException} when the store fails,
 * {@link IllegalArgumentException} for a topic that doesn't exist.
 *
 * <p>A bulk action (revive, discard, cancel, run now) works in batches and takes each row it can
 * lock. A row that stays locked by another transaction, such as a claim or a push, through the
 * action's few re-reads is left as it is; the count it returns says how many it took.
 */
public final class TaskAdmin {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskAdmin.class);

    private static final int BATCH = 1_000;
    /** How often a bulk action reads which matching rows it skipped as locked, and takes them; see {@link #inBatches}. */
    private static final int RESCANS = 3;
    private static final Duration RESCAN_PAUSE = Duration.ofMillis(50);

    private final TaskEngine engine;
    private final TaskStore store;

    TaskAdmin(TaskEngine engine, TaskStore store) {
        this.engine = engine;
        this.store = store;
    }

    /**
     * Deletes a topic that is no longer used, with its history and counts, and
     * stops it on every node. It must have no unfinished tasks: let them finish, or discard its
     * dead letters, first. A node that registers the topic by name creates it again when it
     * starts, so remove it from the configuration before deleting it.
     *
     * @return how many finished tasks were deleted with it
     * @throws IllegalStateException if it has unfinished tasks
     */
    public long deleteTopic(String name) {
        return Unchecked.call("Deleting topic " + name, () -> {
            int id = topicId(name);
            long unfinished = EngineCore.transact(store, transaction -> store.count(transaction, id)).total();
            if (unfinished > 0) {
                throw new IllegalStateException("Topic " + name + " has " + unfinished + " unfinished task(s): let them finish, "
                    + "or discard its dead letters, before deleting it");
            }
            long deleted = 0;
            int batch;
            do {
                batch = EngineCore.transact(store, transaction -> store.purgeAll(transaction, id, BATCH));
                deleted += batch;
            } while (batch == BATCH);
            if (!EngineCore.transact(store, transaction -> store.deleteTopic(transaction, id))) {
                throw new IllegalStateException("Topic " + name + " got new tasks while it was being deleted; it stays");
            }
            if (engine.registersByName(name)) {
                LOGGER.warn("Deleted topic {}, which this node's configuration registers: it is created again when this node starts", name);
            }
            engine.reconcileTopics();
            return deleted;
        });
    }

    /** Every topic in the store, by name. */
    public List<TopicStatus> topics() {
        return Unchecked.call("Listing topics", () -> {
            List<TaskStore.TopicInfo> all = EngineCore.transact(store, store::topics);
            Map<String, TopicSnapshot> counts = new HashMap<>();
            snapshots().forEach(snapshot -> counts.put(snapshot.topic(), snapshot));
            Map<String, String> modules = new HashMap<>();
            engine.effectiveSettings().forEach(settings -> modules.put(settings.topic(), settings.module()));
            Set<String> processing = engine.processing();
            return all.stream()
                .sorted(Comparator.comparing(TaskStore.TopicInfo::name))
                .map(topic -> {
                    TopicSnapshot snapshot = counts.get(topic.name());
                    return new TopicStatus(topic.name(), processing.contains(topic.name()), modules.get(topic.name()),
                        snapshot == null ? null : snapshot.counts(), snapshot == null ? null : snapshot.takenAt());
                })
                .toList();
        });
    }

    /** One topic, if the store has it. */
    public Optional<TopicStatus> topic(String name) {
        return topics().stream().filter(status -> status.name().equals(name)).findFirst();
    }

    /**
     * Every topic's queue counts as the housekeeping leader last recorded them, by topic name. They
     * are at most one snapshot interval old (a minute by default), so dashboards and health checks
     * can read them as often as they like without counting the queue.
     */
    public List<TopicSnapshot> snapshots() {
        return Unchecked.call("Reading queue counts", () -> EngineCore.transact(store, store::snapshots));
    }

    /**
     * Every node registered in the store, this one included, with its last heartbeat and the
     * cluster-wide jobs it leads. A node whose heartbeat is late has stopped or lost the database.
     */
    public List<NodeView> nodes() {
        return Unchecked.call("Listing nodes", () -> EngineCore.transact(store, store::nodes));
    }

    /** A task, finished or not, as pgtasks.task_view shows it. */
    public Optional<TaskView> find(long taskId) {
        return Unchecked.call("Finding task " + taskId, () -> EngineCore.transact(store, transaction -> store.find(transaction, taskId)));
    }

    /** A task's logged attempts, failed and expired ones: its first and its last ten. */
    public List<AttemptView> attempts(long taskId) {
        return Unchecked.call("Reading the attempts of task " + taskId, () -> EngineCore.transact(store, transaction -> store.attempts(transaction, taskId)));
    }

    /** What is failing in a topic: its failing tasks grouped by error, the largest group first. */
    public List<FailureGroup> failing(String topic, int limit) {
        return Unchecked.call("Reading what fails in " + topic, () -> {
            int id = topicId(topic);
            return EngineCore.transact(store, transaction -> store.failing(transaction, id, limit));
        });
    }

    /** A page of a topic's dead letters, lowest id first, after {@code afterId} (0 for the first page). */
    public List<TaskView> deadLetters(String topic, long afterId, int limit) {
        return deadLetters(topic, DeadLetterQuery.ALL.after(afterId), limit);
    }

    /** A page of the dead letters that match. */
    public List<TaskView> deadLetters(String topic, DeadLetterQuery query, int limit) {
        return Unchecked.call("Reading the dead letters of " + topic, () -> {
            int id = topicId(topic);
            return EngineCore.transact(store, transaction -> store.deadLetters(transaction, id, query, limit));
        });
    }

    /**
     * Gives these dead letters another round of attempts, with the topic's whole retry policy.
     * Ids that aren't dead letters of the topic are skipped.
     *
     * @return how many were revived
     */
    public int revive(String topic, Collection<Long> taskIds) {
        return revive(topic, DeadLetterQuery.ids(Objects.requireNonNull(taskIds, "taskIds")));
    }

    /**
     * Revives the dead letters that match, such as those with one error type, first failed in a
     * time range; {@link DeadLetterQuery#ALL} for every one.
     */
    public int revive(String topic, DeadLetterQuery query) {
        return Unchecked.call("Reviving dead letters of " + topic, () -> {
            int id = topicId(topic);
            int revived = inBatches(query, id, (transaction, batch, limit) -> store.revive(transaction, id, batch, limit));
            if (revived > 0) {
                engine.wake(topic);
            }
            return revived;
        });
    }

    /**
     * Gives up on these dead letters: they finish as failed, kept as long as the topic's retention
     * for failed tasks says, and tasks they held back under an ordering run next.
     *
     * @return how many were discarded
     */
    public int discard(String topic, Collection<Long> taskIds) {
        return discard(topic, DeadLetterQuery.ids(Objects.requireNonNull(taskIds, "taskIds")));
    }

    /** Discards the dead letters that match; {@link DeadLetterQuery#ALL} for every one. */
    public int discard(String topic, DeadLetterQuery query) {
        return Unchecked.call("Discarding dead letters of " + topic, () -> {
            int id = topicId(topic);
            int discarded = inBatches(query, id, (transaction, batch, limit) -> store.discard(transaction, id, batch, limit));
            if (discarded > 0) {
                engine.wake(topic);
            }
            return discarded;
        });
    }

    /**
     * A page of a topic's unfinished tasks that match, in any state, lowest id first: by identifier,
     * reference, state, a header and when they were pushed. Page with {@link TaskQuery#after}.
     * Without an identifier, a reference or ids, a search reads the topic's whole backlog, as
     * counting it does.
     */
    public List<TaskView> tasks(String topic, TaskQuery query, int limit) {
        return Unchecked.call("Searching the tasks of " + topic, () -> {
            int id = topicId(topic);
            return EngineCore.transact(store, transaction -> store.tasks(transaction, id, query, limit));
        });
    }

    /**
     * Cancels these unfinished tasks: see {@link #cancel(String, TaskQuery)}. Ids that aren't
     * unfinished tasks of the topic, or are active, are skipped.
     *
     * @return how many were cancelled
     */
    public int cancel(String topic, Collection<Long> taskIds) {
        return cancel(topic, TaskQuery.ids(Objects.requireNonNull(taskIds, "taskIds")));
    }

    /**
     * Takes the matching unfinished tasks out of the queue; {@link TaskQuery#ALL} for every one.
     * They finish as {@link Outcome#CANCELLED}, with the attempts they took, kept as long as the
     * topic's retention for cancelled tasks says, and never run. Ready, delayed, retrying,
     * suspended and dead-lettered tasks are cancelled, and tasks they held back under an ordering
     * run next. An active task is skipped and runs to its end: its node can't be stopped, so
     * marking it cancelled would only make the record disagree with what its handler did.
     *
     * @return how many were cancelled
     */
    public int cancel(String topic, TaskQuery query) {
        return Unchecked.call("Cancelling tasks of " + topic, () -> {
            int id = topicId(topic);
            int cancelled = inBatches(query, id, CANCELLABLE, (transaction, batch, limit) -> store.cancel(transaction, id, batch, limit));
            if (cancelled > 0) {
                LOGGER.info("Cancelled {} task(s) of {}", cancelled, topic);
                engine.wake(topic);
            }
            return cancelled;
        });
    }

    /** Makes these delayed or retrying tasks ready now: see {@link #runNow(String, TaskQuery)}. */
    public int runNow(String topic, Collection<Long> taskIds) {
        return runNow(topic, TaskQuery.ids(Objects.requireNonNull(taskIds, "taskIds")));
    }

    /**
     * Makes the matching delayed and retrying tasks ready at once, keeping their attempts and
     * failures, so a further failure backs off as it would have. Tasks in other states are skipped.
     *
     * @return how many were made ready
     */
    public int runNow(String topic, TaskQuery query) {
        return Unchecked.call("Running tasks of " + topic + " now", () -> {
            int id = topicId(topic);
            int ready = inBatches(query, id, EnumSet.of(QueueState.SCHEDULED, QueueState.RETRYING),
                (transaction, batch, limit) -> store.runNow(transaction, id, batch, limit));
            if (ready > 0) {
                engine.wake(topic);
            }
            return ready;
        });
    }

    /** Deletes a topic's finished tasks with an outcome that finished before a time, as retention does. Returns how many. */
    public long purge(String topic, Outcome outcome, Instant finishedBefore) {
        return Unchecked.call("Purging " + topic, () -> {
            int id = topicId(topic);
            long total = 0;
            while (true) {
                int purged = EngineCore.transact(store, transaction -> store.purge(transaction, id, outcome, finishedBefore, BATCH));
                total += purged;
                if (purged < BATCH) {
                    return total;
                }
            }
        });
    }

    @FunctionalInterface
    private interface Work<Q> {
        List<Long> apply(StoreTransaction transaction, Q query, int limit) throws Exception;
    }

    /** How a bulk action pages through, and re-reads, the rows its query matches. */
    private interface Paging<Q> {
        @Nullable Collection<Long> ids(Q query);

        Q after(Q query, long id);

        Q withIds(Q query, Collection<Long> ids);

        /** The ids of rows that match and that the action would take, read without locking them. */
        List<Long> matching(StoreTransaction transaction, Q query, int limit) throws Exception;
    }

    /** States a cancel takes: every one but active. */
    private static final Set<QueueState> CANCELLABLE = EnumSet.complementOf(EnumSet.of(QueueState.ACTIVE));

    /** As for dead letters, for the tasks in the given states that the action takes. */
    private int inBatches(TaskQuery query, int topicId, Set<QueueState> takes, Work<TaskQuery> work) throws Exception {
        Set<QueueState> states = query.states().isEmpty() ? takes : EnumSet.copyOf(query.states());
        states.retainAll(takes);
        if (states.isEmpty()) {
            return 0; // it asks for states the action doesn't take
        }
        return inBatches(query, work, new Paging<>() {
            @Override
            public @Nullable Collection<Long> ids(TaskQuery query) {
                return query.taskIds();
            }

            @Override
            public TaskQuery after(TaskQuery query, long id) {
                return query.after(id);
            }

            @Override
            public TaskQuery withIds(TaskQuery query, Collection<Long> ids) {
                return query.withIds(ids);
            }

            @Override
            public List<Long> matching(StoreTransaction transaction, TaskQuery query, int limit) throws Exception {
                return store.tasks(transaction, topicId, query.inStates(states), limit).stream().map(TaskView::id).toList();
            }
        });
    }

    private int inBatches(DeadLetterQuery query, int topicId, Work<DeadLetterQuery> work) throws Exception {
        return inBatches(query, work, new Paging<>() {
            @Override
            public @Nullable Collection<Long> ids(DeadLetterQuery query) {
                return query.taskIds();
            }

            @Override
            public DeadLetterQuery after(DeadLetterQuery query, long id) {
                return query.after(id);
            }

            @Override
            public DeadLetterQuery withIds(DeadLetterQuery query, Collection<Long> ids) {
                return new DeadLetterQuery(ids, query.afterId(), query.error(), query.failedFrom(), query.failedUntil());
            }

            @Override
            public List<Long> matching(StoreTransaction transaction, DeadLetterQuery query, int limit) throws Exception {
                return store.deadLetters(transaction, topicId, query, limit).stream().map(TaskView::id).toList();
            }
        });
    }

    /**
     * Given ids in one transaction; otherwise a batch per transaction, so a large topic doesn't
     * hold locks for long. Each batch starts after the last one's highest id: a revived task that
     * fails straight back into the dead letters isn't revived again by the same call.
     *
     * <p>The store skips rows locked at that moment, by a claim, a promotion or a push, and a
     * batch that starts after a skipped row's id never meets it again. So the action then reads
     * which matching rows are left, without locking them, and takes those by id: up to
     * {@value #RESCANS} times, while rows are left. Rows this call has already taken are never
     * taken again, so a revived task that failed straight back is left alone. A row still locked
     * after that is left as it is, and the count says how many were taken.
     */
    private <Q> int inBatches(Q query, Work<Q> work, Paging<Q> paging) throws Exception {
        Set<Long> taken = new HashSet<>();
        Collection<Long> ids = paging.ids(query);
        if (ids == null) {
            Q next = query;
            while (true) {
                Q batch = next;
                List<Long> done = EngineCore.transact(store, transaction -> work.apply(transaction, batch, BATCH));
                taken.addAll(done);
                if (done.size() < BATCH) {
                    break;
                }
                next = paging.after(batch, done.stream().mapToLong(Long::longValue).max().orElseThrow());
            }
        } else if (ids.isEmpty()) {
            return 0;
        } else {
            taken.addAll(EngineCore.transact(store, transaction -> work.apply(transaction, query, ids.size())));
        }
        for (int round = 0; round < RESCANS; round++) {
            if (round > 0) {
                Thread.sleep(RESCAN_PAUSE.toMillis() * round);
            }
            List<Long> left = left(query, paging, taken);
            if (left.isEmpty()) {
                break;
            }
            for (int from = 0; from < left.size(); from += BATCH) {
                List<Long> chunk = left.subList(from, Math.min(left.size(), from + BATCH));
                Q byIds = paging.withIds(query, chunk);
                taken.addAll(EngineCore.transact(store, transaction -> work.apply(transaction, byIds, chunk.size())));
            }
        }
        return taken.size();
    }

    /** The ids of rows the query still matches that this call hasn't taken, read without locks. */
    private <Q> List<Long> left(Q query, Paging<Q> paging, Set<Long> taken) throws Exception {
        List<Long> left = new ArrayList<>();
        Q next = query;
        while (true) {
            Q page = next;
            List<Long> found = EngineCore.transact(store, transaction -> paging.matching(transaction, page, BATCH));
            found.stream().filter(id -> !taken.contains(id)).forEach(left::add);
            if (found.size() < BATCH) {
                return left;
            }
            next = paging.after(page, found.getLast());
        }
    }

    int topicId(String topic) throws Exception {
        return EngineCore.transact(store, transaction -> store.findTopic(transaction, topic))
            .orElseThrow(() -> new IllegalArgumentException("Unknown topic '" + topic + "'"));
    }

    @Override
    public String toString() {
        return "TaskAdmin[" + engine + "]";
    }
}
