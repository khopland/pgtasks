package io.github.khopland.pgtasks.postgres;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.admin.DeadLetterQuery;
import io.github.khopland.pgtasks.spi.TaskStore.ClaimedTask;
import io.github.khopland.pgtasks.spi.TaskStore.Resolution;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Finishing a task must cost the same however much history its topic has. On a new install the
 * completion update was planned once while the task table was small, and Postgres kept that plan for
 * the connection: it walked task_completed_idx by topic alone and matched the ids in a join filter,
 * so every completion read the topic's whole history (the NOTIFY benchmark: a consumer fell to 50 to
 * 100 tasks a second). The drained-queue pass of the TCK didn't see it: its ballast is in another
 * topic, and it explained only plans made for the statement's actual parameters.
 *
 * <p>Here every statement on the way to finishing a task first runs while the tables are small, so
 * any plan Postgres would cache is made then ({@link PlanAudit}), and runs again once the topic has
 * {@value #HISTORY} finished tasks of its own, with the budget of the drained-queue pass.
 */
class CompletionPlanTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresStoreFixture.extension();

    private static final int HISTORY = 50_000;
    private static final int BATCH = 10;

    enum Statistics {
        /** A new install: neither table analyzed yet. */
        NEVER_ANALYZED,
        /** Both analyzed right after the first tasks drained: the queue empty, the history small. */
        ANALYZED_WHILE_DRAINED
    }

    @ParameterizedTest
    @EnumSource(Statistics.class)
    void finishing_a_task_does_not_read_the_topics_history(Statistics statistics, PostgresDatabase database) throws SQLException {
        // One connection, so the plans made while the tables were small are the ones used later.
        PlanAudit audit = new PlanAudit(database.newPool("audited", 1), DrainedQueue.ALLOWED, List.of());
        PostgresStoreFixture fixture = new PostgresStoreFixture(database, audit.dataSource(), TaskSchema.DEFAULT_SCHEMA);
        int topic = fixture.topic("topic");
        int node = fixture.node("node-a");
        database.execute("ALTER TABLE pgtasks.task SET (autovacuum_enabled = false)");
        database.execute("ALTER TABLE pgtasks.queue SET (autovacuum_enabled = false)");

        round(fixture, database, topic, node, "first");
        if (statistics == Statistics.ANALYZED_WHILE_DRAINED) {
            database.execute("DELETE FROM pgtasks.queue");
            database.execute("ANALYZE pgtasks.task");
            database.execute("ANALYZE pgtasks.queue");
            round(fixture, database, topic, node, "second");
        } else {
            assertThat(database.count("SELECT count(*) FROM pg_stat_user_tables WHERE relname IN ('task', 'queue') AND last_analyze IS NULL AND last_autoanalyze IS NULL"))
                .as("neither table has been analyzed").isEqualTo(2);
        }
        // While the tables are this small, scanning them is right; what matters is what happens later.
        int small = audit.findings().size();

        // The topic's finished history, which nothing that finishes a new task needs to read.
        database.execute("""
            INSERT INTO pgtasks.task (topic_id, identifier, payload, outcome, completed_at)
            SELECT %d, 'history-' || g, 'x', 'succeeded', now() - interval '1 day'
            FROM generate_series(1, %d) AS g
            """.formatted(topic, HISTORY));
        int before = audit.audited();
        round(fixture, database, topic, node, "last");

        assertThat(audit.audited() - before).as("statements audited with the history in place").isGreaterThanOrEqualTo(15);
        assertThat(audit.generic()).as("APPEND pushes, explained as the generic plan made in the first round").isPositive();
        assertThat(audit.findings().subList(small, audit.findings().size()))
            .as("statements that read more than " + DrainedQueue.ALLOWED + " with " + HISTORY + " finished tasks in the same topic, " + statistics)
            .isEmpty();
    }

    /** Every way a task leaves the queue or waits in it, for the tasks of one round. */
    private static void round(PostgresStoreFixture fixture, PostgresDatabase database, int topic, int node, String round) throws SQLException {
        fixture.push(topic, Insertion.APPEND, StoreFixture.many(round + "-t", 7 * BATCH));
        finish(fixture, topic, node, StoreFixture.succeeded());
        finish(fixture, topic, node, StoreFixture.filtered("not for us"));
        finish(fixture, topic, node, StoreFixture.retrying(Duration.ofHours(1)));
        finish(fixture, topic, node, StoreFixture.throttled(Duration.ofHours(1)));
        finish(fixture, topic, node, StoreFixture.later(Duration.ofHours(1)));
        finish(fixture, topic, node, StoreFixture.suspension());
        finish(fixture, topic, node, StoreFixture.deadLetter());
        List<Long> discarded = fixture.inTransaction(transaction -> fixture.store().discard(transaction, topic, DeadLetterQuery.ALL, BATCH));
        assertThat(discarded).hasSize(BATCH);

        // A node that died holding claims: the expiry writes an error on each task.
        int dying = fixture.node(round + "-dying");
        fixture.push(topic, Insertion.APPEND, StoreFixture.many(round + "-e", BATCH));
        assertThat(fixture.claim(topic, dying, Ordering.FIFO, BATCH)).hasSize(BATCH);
        database.execute("UPDATE pgtasks.node SET heartbeat_at = now() - interval '1 hour' WHERE id = " + dying);
        assertThat(fixture.requeueExpired(node, Duration.ofMinutes(1))).containsExactly(topic);

        // Pushes that finish, or delete, older tasks of the same identifiers.
        fixture.push(topic, Insertion.SUPERSEDE, StoreFixture.many(round + "-e", BATCH));
        fixture.push(topic, Insertion.REPLACE, StoreFixture.many(round + "-t", BATCH));
        fixture.push(topic, Insertion.DELETE, StoreFixture.many(round + "-t", BATCH));
    }

    private static void finish(PostgresStoreFixture fixture, int topic, int node, Resolution resolution) throws SQLException {
        List<ClaimedTask> tasks = fixture.claim(topic, node, Ordering.FIFO, BATCH);
        assertThat(tasks).hasSize(BATCH);
        fixture.inTransaction(transaction -> {
            fixture.store().complete(transaction, topic, node, tasks.stream().map(task -> StoreFixture.completion(task, resolution)).toList());
            return null;
        });
    }
}
