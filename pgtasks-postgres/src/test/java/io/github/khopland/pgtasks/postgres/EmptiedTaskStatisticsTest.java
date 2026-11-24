package io.github.khopland.pgtasks.postgres;

import java.sql.SQLException;

import io.github.khopland.pgtasks.SchemaMode;
import io.github.khopland.pgtasks.spi.StoreTransaction;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Statistics analyzed right after retention emptied the task table say it has pages but no rows,
 * and every task statement then plans by a tie (the TCK's EMPTIED pass: claims, completions and
 * lookups read all 38,000 tasks). The store repairs them at prepare, which that pass covers, and
 * with every count snapshot the leader takes, covered here.
 */
class EmptiedTaskStatisticsTest {

    private static final String TASK_ROWS = "SELECT reltuples::bigint FROM pg_class WHERE oid = 'pgtasks.task'::regclass";

    @Test
    void the_leaders_count_snapshot_analyzes_a_task_table_whose_statistics_say_it_is_empty() throws SQLException {
        try (DrainedQueue emptied = DrainedQueue.create(TaskSchema.DEFAULT_SCHEMA, true)) {
            PostgresTaskStore store = new PostgresTaskStore(emptied.database().dataSource(), SchemaMode.VALIDATE);
            snapshot(store);
            assertThat(emptied.database().count(TASK_ROWS)).as("the task table's estimate after the snapshot").isGreaterThan(30_000);

            long analyzed = emptied.database().count("SELECT analyze_count FROM pg_stat_user_tables WHERE relid = 'pgtasks.task'::regclass");
            assertThat(analyzed).as("the snapshot's analyze, counted (a copied database starts without statistics)").isOne();
            snapshot(store);
            assertThat(emptied.database().count("SELECT analyze_count FROM pg_stat_user_tables WHERE relid = 'pgtasks.task'::regclass"))
                .as("not analyzed again once the statistics are right").isEqualTo(analyzed);
        }
    }

    @Test
    void a_role_that_does_not_own_the_table_still_takes_its_snapshots() throws SQLException {
        try (DrainedQueue emptied = DrainedQueue.create(TaskSchema.DEFAULT_SCHEMA, true)) {
            String role = "pgtasks_app_" + emptied.database().name().replaceAll("[^a-z0-9_]", "_");
            emptied.database().execute("CREATE ROLE " + role + " LOGIN PASSWORD 'app'");
            try {
                emptied.database().execute("GRANT USAGE ON SCHEMA pgtasks TO " + role);
                emptied.database().execute("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA pgtasks TO " + role);
                PGSimpleDataSource app = new PGSimpleDataSource();
                app.setURL(emptied.database().jdbcUrl());
                app.setUser(role);
                app.setPassword("app");

                snapshot(new PostgresTaskStore(app, SchemaMode.VALIDATE));

                assertThat(emptied.database().count("SELECT count(*) FROM pgtasks.topic_count")).as("snapshots taken").isPositive();
                assertThat(emptied.database().count(TASK_ROWS)).as("left for the owner or autovacuum to analyze").isZero();
            } finally {
                emptied.database().execute("REVOKE ALL ON ALL TABLES IN SCHEMA pgtasks FROM " + role);
                emptied.database().execute("REVOKE ALL ON SCHEMA pgtasks FROM " + role);
                emptied.database().execute("DROP ROLE " + role);
            }
        }
    }

    private static void snapshot(PostgresTaskStore store) throws SQLException {
        try (StoreTransaction transaction = store.begin()) {
            store.snapshotCounts(transaction);
            transaction.commit();
        }
    }
}
