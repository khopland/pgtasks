package org.example.task2.benchmark;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.example.task2.Ordering;

/**
 * The alternative: one table holding unfinished and finished tasks, with the
 * same indexes as the queue design, made partial where that helps, so the comparison is fair.
 * Claims and completions update rows of the large table.
 */
final class SingleTableDesign implements Design {

    private static final String UNFINISHED = "('scheduled', 'retrying', 'ready', 'active', 'suspended', 'failed')";

    private final DataSource dataSource;

    SingleTableDesign(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public String name() {
        return "single table";
    }

    private void sql(String sql) throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    public void create() throws SQLException {
        sql("""
            CREATE SCHEMA single;
            CREATE TYPE single.state AS ENUM ('scheduled', 'retrying', 'ready', 'active', 'suspended', 'failed', 'succeeded', 'filtered', 'redundant');
            CREATE SEQUENCE single.task_id_seq;
            CREATE TABLE single.task (
                topic_id      integer NOT NULL,
                id            bigint NOT NULL,
                identifier    text NOT NULL,
                state         single.state NOT NULL,
                priority      integer NOT NULL DEFAULT 0,
                run_at        timestamptz,
                attempt       integer NOT NULL DEFAULT 0,
                node_id       integer,
                claimed_at    timestamptz,
                payload       text,
                error         jsonb,
                output        text,
                created_at    timestamptz NOT NULL DEFAULT now(),
                completed_at  timestamptz,
                PRIMARY KEY (id)
            ) WITH (autovacuum_vacuum_scale_factor = 0.01);  -- as tasks.task, so sustained load compares like with like
            CREATE INDEX single_ready_idx ON single.task (topic_id, priority DESC, id) WHERE state = 'ready';
            CREATE INDEX single_unfinished_idx ON single.task (topic_id, identifier, id) WHERE state IN %s;
            CREATE INDEX single_identifier_idx ON single.task (topic_id, identifier, id);
            CREATE INDEX single_due_idx ON single.task (run_at) WHERE state IN ('scheduled', 'retrying');
            CREATE INDEX single_active_idx ON single.task (node_id) WHERE state = 'active';
            CREATE INDEX single_created_brin ON single.task USING brin (created_at);
            CREATE INDEX single_completed_idx ON single.task (topic_id, completed_at);
            """.formatted(UNFINISHED));
    }

    @Override
    public void history(long rows) throws SQLException {
        sql("""
            INSERT INTO single.task (topic_id, id, identifier, state, payload, attempt, created_at, completed_at)
            SELECT 1, nextval('single.task_id_seq'), 'h' || (g %% 200000), 'succeeded', repeat('x', 120), 1,
                   now() - make_interval(secs => %d - g), now() - make_interval(secs => %d - g)
            FROM generate_series(1, %d) g
            """.formatted(rows, rows, rows));
    }

    @Override
    public void deadLetters(int rows) throws SQLException {
        sql("""
            INSERT INTO single.task (topic_id, id, identifier, state, payload, attempt, error)
            SELECT 1, nextval('single.task_id_seq'), 'd' || g, 'failed', repeat('x', 120), 5, '{"message":"gave up"}'
            FROM generate_series(1, %d) g
            """.formatted(rows));
    }

    @Override
    public void ready(int rows, String prefix, long first, int identifiers) throws SQLException {
        sql("""
            INSERT INTO single.task (topic_id, id, identifier, state, payload)
            SELECT 1, nextval('single.task_id_seq'), '%s' || ((%d + g) %% %d), 'ready', repeat('x', 120)
            FROM generate_series(1, %d) g
            ORDER BY g
            """.formatted(prefix, first, identifiers, rows));
    }

    @Override
    public List<Claimed> claim(int node, Ordering ordering, int limit, boolean admission) throws SQLException {
        String blocking = switch (ordering) {
            case FIFO -> null;
            case SINGULAR_BY_IDENTIFIER -> "'ready', 'retrying'";
            case SINGULAR_BY_IDENTIFIER_ON_FAILURE -> "'ready', 'retrying', 'failed'";
            case SINGULAR_BY_IDENTIFIER_UNTIL_SUCCESS -> "'ready', 'retrying', 'failed', 'suspended', 'scheduled'";
        };
        // Written like the real store's claim: a fenced per-candidate lookup by identifier, a CASE
        // instead of an OR, an update by ctid, and the plan pinned in the claim's transaction.
        String condition = blocking == null ? "" : """
            AND NOT EXISTS (
                SELECT 1 FROM single.task b
                WHERE b.topic_id = c.topic_id AND b.identifier = c.identifier AND b.id <> c.id AND b.state IN %s
                  AND CASE WHEN b.state = 'active' THEN true WHEN b.id < c.id THEN b.state IN (%s) ELSE false END
                OFFSET 0)
            """.formatted(UNFINISHED, blocking);
        String sql = """
            WITH picked AS MATERIALIZED (
                SELECT c.ctid AS row FROM single.task c
                WHERE c.topic_id = 1 AND c.state = 'ready'
                %s
                ORDER BY c.priority DESC, c.id
                LIMIT ?
                FOR UPDATE OF c SKIP LOCKED
            )
            UPDATE single.task t
            SET state = 'active', node_id = ?, attempt = t.attempt + 1, claimed_at = clock_timestamp()
            WHERE t.topic_id = 1 AND t.ctid = ANY (ARRAY(SELECT row FROM picked))
            RETURNING t.id, t.attempt, t.payload
            """.formatted(condition);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (Statement planning = connection.createStatement(); PreparedStatement statement = connection.prepareStatement(sql)) {
                planning.execute("""
                    SELECT set_config('enable_seqscan', 'off', true), set_config('enable_bitmapscan', 'off', true),
                           set_config('enable_sort', 'off', true), set_config('enable_hashjoin', 'off', true),
                           set_config('enable_mergejoin', 'off', true)
                    """);
                // Same treatment as the real store: plan every claim for its actual parameters.
                statement.unwrap(org.postgresql.PGStatement.class).setPrepareThreshold(0);
                statement.setInt(1, limit);
                statement.setInt(2, node);
                List<Claimed> claimed = new ArrayList<>();
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        claimed.add(new Claimed(resultSet.getLong(1), resultSet.getInt(2)));
                    }
                }
                connection.commit();
                return claimed;
            } finally {
                if (!connection.getAutoCommit()) {
                    connection.rollback();
                    connection.setAutoCommit(true);
                }
            }
        }
    }

    @Override
    public void complete(int node, List<Claimed> claimed) throws SQLException {
        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("""
            UPDATE single.task t
            SET state = 'succeeded', completed_at = now()
            FROM unnest(?::bigint[], ?::integer[]) AS d(id, attempt)
            WHERE t.topic_id = 1 AND t.id = d.id AND t.attempt = d.attempt AND t.node_id = ? AND t.state = 'active'
            """)) {
            Array ids = connection.createArrayOf("bigint", claimed.stream().map(Claimed::id).toArray());
            Array attempts = connection.createArrayOf("integer", claimed.stream().map(Claimed::attempt).toArray());
            statement.setArray(1, ids);
            statement.setArray(2, attempts);
            statement.setInt(3, node);
            if (statement.executeUpdate() != claimed.size()) {
                throw new IllegalStateException("stale claim");
            }
        }
    }

    @Override
    public int purgeOldest(int rows) throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            return statement.executeUpdate("""
                WITH doomed AS MATERIALIZED (
                    SELECT id FROM single.task
                    WHERE topic_id = 1 AND state = 'succeeded' AND completed_at < now()
                    ORDER BY completed_at
                    LIMIT %d
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM single.task t USING doomed WHERE t.id = doomed.id
                """.formatted(rows));
        }
    }

    @Override
    public List<String> tables() {
        return List.of("single.task");
    }

    @Override
    public long bytes() throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT pg_total_relation_size('single.task')")) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    @Override
    public void analyze() throws SQLException {
        sql("ANALYZE single.task");
    }

    @Override
    public void vacuum() throws SQLException {
        sql("VACUUM ANALYZE single.task");
    }
}
