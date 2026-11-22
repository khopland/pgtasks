package io.github.khopland.pgtasks.engine;

import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.github.khopland.pgtasks.Outcome;
import io.github.khopland.pgtasks.TaskStorageException;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.AttemptView;
import io.github.khopland.pgtasks.admin.DeadLetterQuery;
import io.github.khopland.pgtasks.admin.FailureGroup;
import io.github.khopland.pgtasks.admin.NodeView;
import io.github.khopland.pgtasks.admin.Schedule;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.admin.TopicSnapshot;
import io.github.khopland.pgtasks.admin.TopicStatus;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The operator's side of an engine, from {@link TaskEngine#admin()}: topics, tasks, dead letters
 * and retention. Everything here reads or changes the store directly, so it works on a node that
 * only pushes, and acts on the whole cluster: a topic paused here is paused on every node.
 * Failures are unchecked: {@link TaskStorageException} when the store fails,
 * {@link IllegalArgumentException} for a topic that doesn't exist.
 */
public final class TaskAdmin {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskAdmin.class);

    private static final int BATCH = 1_000;

    private final TaskEngine engine;
    private final TaskStore store;

    TaskAdmin(TaskEngine engine, TaskStore store) {
        this.engine = engine;
        this.store = store;
    }

    /**
     * Registers a topic, so every node with a handler for it starts processing it: one registered
     * by name, or matching a pattern. Idempotent.
     */
    public void createTopic(String name) {
        TopicRef.of(name, Object.class);
        Unchecked.run("Creating topic " + name, () -> {
            EngineCore.transact(store, transaction -> store.createTopic(transaction, name));
            engine.reconcileTopics();
        });
    }

    /**
     * Deletes a topic that is no longer used, with its history, recurring tasks and counts, and
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

    /** Pauses a topic on every node: pushes are still taken, nothing is claimed. Running batches finish. */
    public void pause(String topic) {
        setPaused(topic, true);
    }

    /** Resumes a paused topic on every node. */
    public void resume(String topic) {
        setPaused(topic, false);
    }

    private void setPaused(String topic, boolean paused) {
        Unchecked.run((paused ? "Pausing " : "Resuming ") + topic, () -> {
            if (!EngineCore.transact(store, transaction -> store.setPaused(transaction, topic, paused))) {
                throw new IllegalArgumentException("Unknown topic '" + topic + "'");
            }
            engine.reconcileTopics();
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
            Set<String> open = Set.copyOf(engine.openBreakers());
            return all.stream()
                .sorted(Comparator.comparing(TaskStore.TopicInfo::name))
                .map(topic -> {
                    TopicSnapshot snapshot = counts.get(topic.name());
                    return new TopicStatus(topic.name(), topic.paused(), processing.contains(topic.name()), modules.get(topic.name()),
                        snapshot == null ? null : snapshot.counts(), snapshot == null ? null : snapshot.takenAt(), open.contains(topic.name()));
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

    /** Every topic's recurring tasks as stored, with their next and last runs. */
    public List<Schedule> schedules() {
        return Unchecked.call("Reading recurring tasks", () -> EngineCore.transact(store, store::schedules));
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
            int revived = inBatches(query, (transaction, batch, limit) -> store.revive(transaction, id, batch, limit));
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
            int discarded = inBatches(query, (transaction, batch, limit) -> store.discard(transaction, id, batch, limit));
            if (discarded > 0) {
                engine.wake(topic);
            }
            return discarded;
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
    private interface DeadLetterWork {
        List<Long> apply(StoreTransaction transaction, DeadLetterQuery query, int limit) throws Exception;
    }

    /**
     * Given ids in one transaction; otherwise a batch per transaction, so a large topic doesn't
     * hold locks for long. Each batch starts after the last one's highest id: a revived task that
     * fails straight back into the dead letters isn't revived again by the same call.
     */
    private int inBatches(DeadLetterQuery query, DeadLetterWork work) throws Exception {
        Collection<Long> ids = query.taskIds();
        if (ids != null) {
            return ids.isEmpty() ? 0 : EngineCore.transact(store, transaction -> work.apply(transaction, query, ids.size())).size();
        }
        int total = 0;
        DeadLetterQuery next = query;
        while (true) {
            DeadLetterQuery batch = next;
            List<Long> done = EngineCore.transact(store, transaction -> work.apply(transaction, batch, BATCH));
            total += done.size();
            if (done.size() < BATCH) {
                return total;
            }
            next = batch.after(done.stream().mapToLong(Long::longValue).max().orElseThrow());
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
