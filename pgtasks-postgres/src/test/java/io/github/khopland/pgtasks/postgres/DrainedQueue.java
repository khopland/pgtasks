package io.github.khopland.pgtasks.postgres;

import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;

import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.TestPostgres;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A queue whose statistics say it is empty while it isn't: the normal state right after a burst
 * drained, when autovacuum tends to analyze it. Until the next analyze the planner expects no rows
 * at all, every plan looks free, and the cheapest-looking one can read the whole queue, or worse,
 * the whole queue once per row (the Phase 2 benchmark's SINGULAR collapse at 1M rows).
 *
 * <p>Set up before a test: a burst is pushed and drained, the queue is analyzed and its autovacuum
 * turned off, so the statistics stay "empty"; then live ballast goes in, in a topic of its own, in
 * every waiting state, next to the burst's finished history. A statement that reads only what it
 * needs never touches the ballast. Every statement the store runs is audited ({@link PlanAudit}),
 * and {@link #check()} fails if one read more than {@link #ALLOWED} of the queue and task tables.
 */
final class DrainedQueue implements AutoCloseable {

    static final String BALLAST_TOPIC = "ballast";

    /**
     * What one statement in a TCK test may read of the queue and task tables. In the TCK the
     * largest claims examine 90 rows in under 160 buffers. Reading the ballast takes thousands of
     * rows, or, walking an index over it, over 300 buffers for a handful of rows.
     */
    static final PlanAudit.Budget ALLOWED = new PlanAudit.Budget(1_000, 64, 4);

    private static final int BURST = 20_000;
    /** A sixth in each of ready (twice), suspended, failed, scheduled and retrying: over 3,000 per state. */
    private static final int BALLAST = 20_000;

    /**
     * Statements that read every unfinished task by design. Nothing else belongs here: a statement
     * on a claim, completion or push path has to find its rows by index whatever the statistics say.
     */
    private static final List<String> READS_EVERYTHING = List.of(
        // Count snapshots: every topic's counts, once a minute on the housekeeping leader.
        "INSERT INTO tasks.topic_count"
    );

    private final PostgresDatabase database;
    private final PlanAudit audit;

    private DrainedQueue(PostgresDatabase database) {
        this.database = database;
        this.audit = new PlanAudit(database.dataSource(), ALLOWED, READS_EVERYTHING);
    }

    /**
     * A database of its own with the drained queue, copied from a template built once per JVM:
     * building the ballast takes over a second, a copy a fraction of that.
     */
    static DrainedQueue create() {
        DrainedQueue drained = new DrainedQueue(TestPostgres.createDatabase("drained-queue", DrainedQueue::build));
        assertThat(drained.database.count("SELECT reltuples::bigint FROM pg_class WHERE oid = 'tasks.queue'::regclass"))
            .as("the queue's statistics say it is empty").isLessThanOrEqualTo(0);
        return drained;
    }

    private static void build(PostgresDatabase database) throws SQLException {
        TaskSchema.migrate(database.dataSource());
        database.execute("SELECT tasks.create_topic('" + BALLAST_TOPIC + "')");
        // A burst that came and went: the queue keeps its pages, and the analyze finds no live row.
        database.execute("""
            WITH t AS (
                INSERT INTO tasks.task (topic_id, identifier, outcome, completed_at)
                SELECT (SELECT id FROM tasks.topic WHERE name = 'ballast'), 'burst-' || g, 'succeeded', now() - interval '1 year'
                FROM generate_series(1, %d) AS g
                RETURNING id, topic_id, identifier
            )
            INSERT INTO tasks.queue (task_id, topic_id, identifier, state)
            SELECT id, topic_id, identifier, 'ready' FROM t
            """.formatted(BURST));
        database.execute("DELETE FROM tasks.queue");
        database.execute("ANALYZE tasks.queue");
        database.execute("ALTER TABLE tasks.queue SET (autovacuum_enabled = false)");
        // Live ballast the statistics don't know about. None of it active: TCK tests expire other
        // nodes' claims and would requeue it.
        database.execute("""
            WITH t AS (
                INSERT INTO tasks.task (topic_id, identifier, payload)
                SELECT (SELECT id FROM tasks.topic WHERE name = 'ballast'), 'ballast-' || (g %% 2000), 'x'
                FROM generate_series(1, %d) AS g
                RETURNING id, topic_id, identifier
            )
            INSERT INTO tasks.queue (task_id, topic_id, identifier, state, run_at)
            SELECT id, topic_id, identifier,
                   (ARRAY['ready', 'ready', 'suspended', 'failed', 'scheduled', 'retrying'])[1 + id %% 6]::tasks.queue_state,
                   CASE WHEN id %% 6 >= 4 THEN now() + interval '100 years' END
            FROM t
            """.formatted(BALLAST));
    }

    PostgresDatabase database() {
        return database;
    }

    PlanAudit audit() {
        return audit;
    }

    /** For the store: every statement it runs is audited. */
    DataSource dataSource() {
        return audit.dataSource();
    }

    /** Fails if a store statement read more than allowed, with its plan. */
    void check() {
        assertThat(audit.audited()).as("statements audited").isPositive();
        assertThat(audit.findings())
            .as("statements that read more than " + ALLOWED + " while the queue's statistics said it was empty")
            .isEmpty();
    }

    @Override
    public void close() {
        database.close();
    }
}
