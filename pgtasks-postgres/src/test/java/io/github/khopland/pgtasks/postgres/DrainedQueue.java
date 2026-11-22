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
 * every waiting state, next to the burst's finished history. And the topics the TCK uses already
 * have finished tasks of their own, so a statement that finds a topic's tasks by topic alone reads
 * them too (on a table that was never analyzed, the completion update did: the NOTIFY benchmark).
 * A statement that reads only what it needs never touches the ballast. Every statement the store
 * runs is audited ({@link PlanAudit}), and {@link #check()} fails if one read more than
 * {@link #ALLOWED} of the queue and task tables.
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
        "INSERT INTO pgtasks.topic_count"
    );

    /**
     * The topics TCK tests and the Postgres tests run on, each with {@link #TOPIC_HISTORY} finished
     * tasks before the test starts. Not "retired" and "busy": their tests check that purging and
     * deleting a topic see exactly the topic's own tasks.
     */
    private static final List<String> HISTORY_TOPICS = List.of("topic", "other", "first", "second", "reports", "merge", "idle", "empty", "unset");
    /** Enough that reading one topic's history takes more than {@link #ALLOWED}. */
    private static final int TOPIC_HISTORY = 2_000;

    private final PostgresDatabase database;
    private final String schema;
    private final boolean tasksEmptied;
    private final PlanAudit audit;

    private DrainedQueue(PostgresDatabase database, String schema, boolean tasksEmptied) {
        this.database = database;
        this.schema = schema;
        this.tasksEmptied = tasksEmptied;
        this.audit = new PlanAudit(database.dataSource(), ALLOWED, READS_EVERYTHING.stream().map(sql -> TaskSchema.qualify(sql, schema)).toList());
    }

    /**
     * A database of its own with the drained queue in the given schema, copied from a template built
     * once per JVM and schema: building the ballast takes over a second, a copy a fraction of that.
     *
     * <p>With {@code tasksEmptied}, the burst's tasks are deleted too before both tables are
     * analyzed, so the task table's statistics also say it is empty while history and ballast sit
     * in it: a new installation's first analyze, or one after retention purged every finished task.
     */
    static DrainedQueue create(String schema, boolean tasksEmptied) {
        String template = (tasksEmptied ? "emptied-tables-" : "drained-queue-") + schema;
        DrainedQueue drained = new DrainedQueue(TestPostgres.createDatabase(template, database -> build(database, schema, tasksEmptied)), schema,
            tasksEmptied);
        assertThat(drained.database.count(TaskSchema.qualify("SELECT reltuples::bigint FROM pg_class WHERE oid = 'pgtasks.queue'::regclass", schema)))
            .as("the queue's statistics say it is empty").isLessThanOrEqualTo(0);
        if (tasksEmptied) {
            assertThat(drained.database.count(TaskSchema.qualify("SELECT reltuples::bigint FROM pg_class WHERE oid = 'pgtasks.task'::regclass", schema)))
                .as("the task table's statistics say it is empty").isZero();
        }
        assertThat(drained.database.count("SELECT count(*) FROM pg_namespace WHERE nspname IN ('pgtasks', '" + schema + "')"))
            .as("only the " + schema + " schema exists, so a statement that ignores the configured name fails").isOne();
        return drained;
    }

    private static void build(PostgresDatabase database, String schema, boolean tasksEmptied) throws SQLException {
        TaskSchema.migrate(database.dataSource(), schema);
        execute(database, schema, "SELECT pgtasks.create_topic('" + BALLAST_TOPIC + "')");
        // A burst that came and went: the queue keeps its pages, and the analyze finds no live row.
        execute(database, schema, """
            WITH t AS (
                INSERT INTO pgtasks.task (topic_id, identifier, outcome, completed_at)
                SELECT (SELECT id FROM pgtasks.topic WHERE name = 'ballast'), 'burst-' || g, 'succeeded', now() - interval '1 year'
                FROM generate_series(1, %d) AS g
                RETURNING id, topic_id, identifier
            )
            INSERT INTO pgtasks.queue (task_id, topic_id, identifier, state)
            SELECT id, topic_id, identifier, 'ready' FROM t
            """.formatted(BURST));
        execute(database, schema, "DELETE FROM pgtasks.queue");
        execute(database, schema, "ANALYZE pgtasks.queue");
        execute(database, schema, "ALTER TABLE pgtasks.queue SET (autovacuum_enabled = false)");
        if (tasksEmptied) {
            execute(database, schema, "DELETE FROM pgtasks.task");
            execute(database, schema, "ANALYZE pgtasks.task");
            execute(database, schema, "ALTER TABLE pgtasks.task SET (autovacuum_enabled = false)");
        }
        // Live ballast the statistics don't know about. None of it active: TCK tests expire other
        // nodes' claims and would requeue it.
        execute(database, schema, """
            WITH t AS (
                INSERT INTO pgtasks.task (topic_id, identifier, payload)
                SELECT (SELECT id FROM pgtasks.topic WHERE name = 'ballast'), 'ballast-' || (g %% 2000), 'x'
                FROM generate_series(1, %d) AS g
                RETURNING id, topic_id, identifier
            )
            INSERT INTO pgtasks.queue (task_id, topic_id, identifier, state, run_at)
            SELECT id, topic_id, identifier,
                   (ARRAY['ready', 'ready', 'suspended', 'failed', 'scheduled', 'retrying'])[1 + id %% 6]::pgtasks.queue_state,
                   CASE WHEN id %% 6 >= 4 THEN now() + interval '100 years' END
            FROM t
            """.formatted(BALLAST));
        // History in the tests' own topics, finished in a hundred years: retention, whose cutoffs are
        // near now, never purges it. Every outcome, under identifiers no test uses.
        for (String topic : HISTORY_TOPICS) {
            execute(database, schema, "SELECT pgtasks.create_topic('" + topic + "')");
        }
        execute(database, schema, """
            INSERT INTO pgtasks.task (topic_id, identifier, payload, outcome, completed_at)
            SELECT tp.id, 'history-' || (g %% 500), 'x',
                   (ARRAY['succeeded', 'succeeded', 'filtered', 'failed', 'redundant'])[1 + g %% 5]::pgtasks.task_outcome,
                   now() + interval '100 years'
            FROM pgtasks.topic tp, generate_series(1, %d) AS g
            WHERE tp.name = ANY ('%s'::text[])
            """.formatted(TOPIC_HISTORY, "{" + String.join(",", HISTORY_TOPICS) + "}"));
    }

    /** SQL written for the default schema, on the given one. */
    private static void execute(PostgresDatabase database, String schema, String sql) {
        database.execute(TaskSchema.qualify(sql, schema));
    }

    PostgresDatabase database() {
        return database;
    }

    String schema() {
        return schema;
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
            .as("statements that read more than " + ALLOWED + " while the " + (tasksEmptied ? "queue's and task table's" : "queue's")
                + " statistics said it was empty")
            .isEmpty();
    }

    @Override
    public void close() {
        database.close();
    }
}
