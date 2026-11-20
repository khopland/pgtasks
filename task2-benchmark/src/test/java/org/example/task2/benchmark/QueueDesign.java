package org.example.task2.benchmark;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.example.task2.Ordering;
import org.example.task2.postgres.PostgresTaskStore;
import org.example.task2.postgres.TaskSchema;
import org.example.task2.spi.StoreTransaction;
import org.example.task2.spi.TaskStore;

/** The Task 2.0 design: a small queue table for unfinished tasks, history in tasks.task. Uses the real store. */
final class QueueDesign implements Design {

    private final DataSource dataSource;
    private final PostgresTaskStore store;
    private int topic, node;

    QueueDesign(DataSource dataSource) {
        this.dataSource = dataSource;
        this.store = new PostgresTaskStore(dataSource);
    }

    @Override
    public String name() {
        return "queue + task";
    }

    @Override
    public void create() throws Exception {
        TaskSchema.migrate(dataSource);
        try (StoreTransaction transaction = store.begin()) {
            topic = store.createTopic(transaction, "bench");
            node = store.registerNode(transaction, "bench", "bench");
            store.ensureAdmission(transaction, "bench", 1_000_000);
            transaction.commit();
        }
    }

    private void sql(String sql) throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute(sql.replace(":topic", String.valueOf(topic)));
        }
    }

    @Override
    public void history(long rows) throws SQLException {
        sql("""
            INSERT INTO tasks.task (topic_id, id, identifier, payload, created_at, outcome, attempts, completed_at)
            SELECT :topic, nextval('tasks.task_id_seq'), 'h' || (g %% 200000), repeat('x', 120),
                   now() - make_interval(secs => %d - g), 'succeeded', 1, now() - make_interval(secs => %d - g)
            FROM generate_series(1, %d) g
            """.formatted(rows, rows, rows));
    }

    @Override
    public void deadLetters(int rows) throws SQLException {
        sql("""
            WITH stored AS (
                INSERT INTO tasks.task (topic_id, id, identifier, payload, error, attempts)
                SELECT :topic, nextval('tasks.task_id_seq'), 'd' || g, repeat('x', 120), '{"message":"gave up"}', 5
                FROM generate_series(1, %d) g
                RETURNING id, identifier
            )
            INSERT INTO tasks.queue (task_id, topic_id, identifier, state, attempt)
            SELECT id, :topic, identifier, 'failed', 5 FROM stored
            """.formatted(rows));
    }

    @Override
    public void ready(int rows, String prefix, int identifiers) throws SQLException {
        sql("""
            WITH stored AS (
                INSERT INTO tasks.task (topic_id, id, identifier, payload)
                SELECT :topic, nextval('tasks.task_id_seq'), '%s' || (g %% %d), repeat('x', 120)
                FROM generate_series(1, %d) g
                ORDER BY g
                RETURNING id, identifier
            )
            INSERT INTO tasks.queue (task_id, topic_id, identifier, state)
            SELECT id, :topic, identifier, 'ready' FROM stored
            """.formatted(prefix, identifiers, rows));
    }

    @Override
    public List<Claimed> claim(int ignored, Ordering ordering, int limit, boolean admission) throws SQLException {
        try (StoreTransaction transaction = store.begin()) {
            TaskStore.Claim claim = store.claim(transaction, new TaskStore.ClaimRequest(topic, node, ordering, limit,
                admission ? new TaskStore.Admission("bench", 1e9, 1_000_000) : null));
            transaction.commit();
            return claim.tasks().stream().map(task -> new Claimed(task.id(), task.attempt())).toList();
        }
    }

    @Override
    public void complete(int ignored, List<Claimed> claimed) throws SQLException {
        try (StoreTransaction transaction = store.begin()) {
            store.complete(transaction, topic, node, claimed.stream()
                .map(each -> new TaskStore.Completion(each.id(), each.attempt(), null, new TaskStore.Resolution.Finished(TaskStore.Outcome.SUCCEEDED, null)))
                .toList());
            transaction.commit();
        }
    }

    @Override
    public long bytes() throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             var resultSet = statement.executeQuery("""
                 SELECT sum(pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'tasks' AND c.relkind = 'r'
                 """)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    @Override
    public void vacuum() throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("VACUUM ANALYZE tasks.task");
            statement.execute("VACUUM ANALYZE tasks.queue");
        }
    }
}
