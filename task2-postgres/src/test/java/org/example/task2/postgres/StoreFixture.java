package org.example.task2.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

import org.example.task2.Insertion;
import org.example.task2.Ordering;
import org.example.task2.Pushed;
import org.example.task2.spi.TaskStore;
import org.example.task2.spi.TaskStore.ClaimedTask;
import org.example.task2.spi.TaskStore.Completion;
import org.example.task2.spi.TaskStore.NewTask;
import org.example.task2.spi.TaskStore.Resolution;
import org.example.task2.spi.TaskStore.WaitState;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;

/** Drives the store directly, each call in its own auto-committed statement. */
final class StoreFixture {

    static PostgresExtension extension() {
        return PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));
    }

    final PostgresDatabase database;
    final PostgresTaskStore store = new PostgresTaskStore();

    StoreFixture(PostgresDatabase database) {
        this.database = database;
    }

    interface Work<T> {
        T run(Connection connection) throws SQLException;
    }

    <T> T inTransaction(Work<T> work) throws SQLException {
        try (Connection connection = database.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = work.run(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException | Error e) {
                connection.rollback();
                throw e;
            }
        }
    }

    int topic(String name) throws SQLException {
        return inTransaction(connection -> store.createTopic(connection, name));
    }

    int node(String name) throws SQLException {
        return inTransaction(connection -> store.registerNode(connection, name, "test"));
    }

    static NewTask task(String identifier) {
        return new NewTask(identifier, "payload-" + identifier, null, null, null, false, 0, null, null, null);
    }

    static NewTask delayed(String identifier, Duration delay) {
        return new NewTask(identifier, null, null, null, delay, false, 0, null, null, null);
    }

    static NewTask suspended(String identifier) {
        return new NewTask(identifier, null, null, null, null, true, 0, null, null, null);
    }

    static NewTask prioritized(String identifier, int priority) {
        return new NewTask(identifier, null, null, null, null, false, priority, null, null, null);
    }

    static NewTask deduplicated(String identifier, String key) {
        return new NewTask(identifier, null, null, null, null, false, 0, key, null, null);
    }

    List<Pushed> push(int topic, Insertion insertion, NewTask... tasks) throws SQLException {
        return inTransaction(connection -> store.push(connection, topic, insertion, List.of(tasks)));
    }

    long push(int topic, String identifier) throws SQLException {
        return push(topic, Insertion.APPEND, task(identifier)).getFirst().id();
    }

    List<ClaimedTask> claim(int topic, int node, Ordering ordering, int limit) throws SQLException {
        return inTransaction(connection -> store.claim(connection, new TaskStore.ClaimRequest(topic, node, ordering, limit, null)).tasks());
    }

    List<ClaimedTask> claim(int topic, int node) throws SQLException {
        return claim(topic, node, Ordering.FIFO, 100);
    }

    void complete(int topic, int node, ClaimedTask task, Resolution resolution) throws SQLException {
        inTransaction(connection -> {
            store.complete(connection, topic, node, List.of(new Completion(task.id(), task.attempt(), task.claimedAt(), resolution)));
            return null;
        });
    }

    static Resolution succeeded() {
        return new Resolution.Finished(TaskStore.Outcome.SUCCEEDED, null);
    }

    static Resolution retrying(Duration delay) {
        return new Resolution.Waiting(WaitState.RETRYING, delay, true, "{\"message\":\"boom\"}");
    }

    static Resolution deadLetter() {
        return new Resolution.Waiting(WaitState.DEAD_LETTER, null, true, "{\"message\":\"gave up\"}");
    }

    static Resolution suspension() {
        return new Resolution.Waiting(WaitState.SUSPENDED, null, false, null);
    }

    static Resolution later(Duration delay) {
        return new Resolution.Waiting(WaitState.SCHEDULED, delay, false, null);
    }

    static Resolution throttled(Duration delay) {
        return new Resolution.Waiting(WaitState.RETRYING, delay, false, null);
    }

    /** The state of a task as tasks.task_view shows it. */
    String state(long task) {
        List<List<Object>> rows = database.rows("SELECT state FROM tasks.task_view WHERE id = ?", task);
        return rows.isEmpty() ? null : (String) rows.getFirst().getFirst();
    }

    int attempt(long task) {
        return ((Number) database.rows("SELECT attempt FROM tasks.task_view WHERE id = ?", task).getFirst().getFirst()).intValue();
    }

    void expireNode(int node) {
        database.execute("UPDATE tasks.node SET heartbeat_at = now() - interval '1 hour' WHERE id = " + node);
    }

    void makeDue() {
        database.execute("UPDATE tasks.queue SET run_at = now() - interval '1 second' WHERE run_at IS NOT NULL");
    }

    static List<Long> ids(List<ClaimedTask> tasks) {
        return tasks.stream().map(ClaimedTask::id).toList();
    }

    static NewTask[] many(String prefix, int count) {
        return IntStream.range(0, count).mapToObj(index -> task(prefix + index)).toArray(NewTask[]::new);
    }
}
