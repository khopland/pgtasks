package io.github.khopland.pgtasks.test.tck;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.admin.AttemptView;
import io.github.khopland.pgtasks.admin.QueueCounts;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;
import io.github.khopland.pgtasks.spi.TaskStore.ClaimedTask;
import io.github.khopland.pgtasks.spi.TaskStore.Completion;
import io.github.khopland.pgtasks.spi.TaskStore.NewTask;
import io.github.khopland.pgtasks.spi.TaskStore.Resolution;
import io.github.khopland.pgtasks.spi.TaskStore.WaitState;

import org.jspecify.annotations.Nullable;

/**
 * Drives a task store for the TCK, each call in its own committed transaction. A store's test
 * provides the store and a way to move its time forward.
 */
public abstract class StoreFixture implements AutoCloseable {

    public abstract TaskStore store();

    /**
     * Moves the store's time forward: due times, heartbeats and rate-limit refills all see the
     * jump. Postgres backdates its timestamps; the in-memory store advances its clock.
     */
    public abstract void advance(Duration duration) throws Exception;

    @Override
    public void close() {
    }

    @FunctionalInterface
    public interface Work<T extends @Nullable Object> {
        T run(StoreTransaction transaction) throws SQLException;
    }

    public <T extends @Nullable Object> T inTransaction(Work<T> work) throws SQLException {
        try (StoreTransaction transaction = store().begin()) {
            T result = work.run(transaction);
            transaction.commit();
            return result;
        }
    }

    public int topic(String name) throws SQLException {
        return inTransaction(transaction -> store().createTopic(transaction, name));
    }

    public int node(String name) throws SQLException {
        return inTransaction(transaction -> store().registerNode(transaction, name, "test"));
    }

    public void heartbeat(int node) throws SQLException {
        inTransaction(transaction -> {
            store().heartbeat(transaction, node);
            return null;
        });
    }

    /** The id of a task the push stored, as queued or redundant. */
    public static long id(Pushed pushed) {
        return Objects.requireNonNull(pushed.id(), () -> pushed + " stored no task");
    }

    public static NewTask task(String identifier) {
        return new NewTask(identifier, "payload-" + identifier, null, null, null, false, 0, null, null, null, null);
    }

    public static NewTask delayed(String identifier, Duration delay) {
        return new NewTask(identifier, null, null, null, delay, false, 0, null, null, null, null);
    }

    public static NewTask suspended(String identifier) {
        return new NewTask(identifier, null, null, null, null, true, 0, null, null, null, null);
    }

    public static NewTask prioritized(String identifier, int priority) {
        return new NewTask(identifier, null, null, null, null, false, priority, null, null, null, null);
    }

    public static NewTask deduplicated(String identifier, String key) {
        return new NewTask(identifier, null, null, null, null, false, 0, key, null, null, null);
    }

    public static NewTask[] many(String prefix, int count) {
        return IntStream.range(0, count).mapToObj(index -> task(prefix + index)).toArray(NewTask[]::new);
    }

    public List<Pushed> push(int topic, Insertion insertion, NewTask... tasks) throws SQLException {
        return inTransaction(transaction -> store().push(transaction, topic, insertion, List.of(tasks)));
    }

    public long push(int topic, String identifier) throws SQLException {
        return id(push(topic, Insertion.APPEND, task(identifier)).getFirst());
    }

    public List<ClaimedTask> claim(int topic, int node, Ordering ordering, int limit) throws SQLException {
        return inTransaction(transaction -> store().claim(transaction, new TaskStore.ClaimRequest(topic, node, ordering, limit, null)).tasks());
    }

    public List<ClaimedTask> claim(int topic, int node) throws SQLException {
        return claim(topic, node, Ordering.FIFO, 100);
    }

    public void complete(int topic, int node, ClaimedTask task, Resolution resolution) throws SQLException {
        inTransaction(transaction -> {
            store().complete(transaction, topic, node, List.of(completion(task, resolution)));
            return null;
        });
    }

    public static Completion completion(ClaimedTask task, Resolution resolution) {
        return new Completion(task.id(), task.attempt(), task.claimedAt(), resolution);
    }

    /** Requeues expired tasks as ready, without counting the expiry against them. */
    public List<Integer> requeueExpired(int node, Duration expiry) throws SQLException {
        return requeueExpired(node, expiry, TaskStore.Expiries.UNCOUNTED);
    }

    public List<Integer> requeueExpired(int node, Duration expiry, TaskStore.Expiries expiries) throws SQLException {
        return inTransaction(transaction -> store().requeueExpired(transaction, node, expiry, expiries));
    }

    /** Promotes what is due and returns the topics that got work. */
    public List<Integer> promoteDue() throws SQLException {
        return inTransaction(transaction -> store().promoteDue(transaction, 1000)).topics();
    }

    public static Resolution succeeded() {
        return new Resolution.Finished(TaskStore.Outcome.SUCCEEDED, null);
    }

    public static Resolution filtered(String reason) {
        return new Resolution.Finished(TaskStore.Outcome.FILTERED, reason);
    }

    public static Resolution retrying(Duration delay) {
        return new Resolution.Waiting(WaitState.RETRYING, delay, true, "{\"message\":\"boom\"}");
    }

    public static Resolution deadLetter() {
        return new Resolution.Waiting(WaitState.DEAD_LETTER, null, true, "{\"message\":\"gave up\"}");
    }

    public static Resolution suspension() {
        return new Resolution.Waiting(WaitState.SUSPENDED, null, false, null);
    }

    public static Resolution later(Duration delay) {
        return new Resolution.Waiting(WaitState.SCHEDULED, delay, false, null);
    }

    public static Resolution throttled(Duration delay) {
        return new Resolution.Waiting(WaitState.RETRYING, delay, false, null);
    }

    public Optional<TaskView> view(long task) throws SQLException {
        return inTransaction(transaction -> store().find(transaction, task));
    }

    /** The state as tasks.task_view shows it, or null if the task is gone. */
    public @Nullable String state(long task) {
        try {
            return view(task).map(TaskView::state).orElse(null);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public int attempt(long task) {
        try {
            return view(task).orElseThrow().attempt();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public List<AttemptView> attempts(long task) throws SQLException {
        return inTransaction(transaction -> store().attempts(transaction, task));
    }

    public QueueCounts counts(int topic) throws SQLException {
        return inTransaction(transaction -> store().count(transaction, topic));
    }

    public static List<Long> ids(List<ClaimedTask> tasks) {
        return tasks.stream().map(ClaimedTask::id).toList();
    }
}
