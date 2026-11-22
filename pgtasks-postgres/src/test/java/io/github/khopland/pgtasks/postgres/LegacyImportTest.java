package io.github.khopland.pgtasks.postgres;

import java.util.Arrays;
import java.util.List;

import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Review 2026-11-21: no way to move a 1.x application's queued tasks to 2.0. */
class LegacyImportTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresStoreFixture.extension();

    /** The 1.x table as task-jdbc's changelog creates it on Postgres, unpartitioned (the importer only reads it). */
    private static void legacyTable(PostgresDatabase database) {
        database.execute("""
            CREATE TABLE task (
                topic       varchar(250) NOT NULL,
                sequence    bigserial NOT NULL,
                identifier  varchar(500) NOT NULL,
                state       numeric(1) NOT NULL DEFAULT 1 CHECK (state BETWEEN 0 AND 8),
                descent     numeric(1) NOT NULL DEFAULT 1 CHECK (descent BETWEEN 1 AND 6),
                owner       varchar(250),
                created     timestamp NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
                completed   timestamp,
                input       text,
                output      text,
                reference   varchar(500),
                scheduled   timestamp,
                PRIMARY KEY (topic, sequence)
            )
            """);
        // ready, ready, failed, suspended, succeeded, delayed into the future, active, and another topic
        database.execute("""
            INSERT INTO task (topic, identifier, state, input, reference, scheduled, completed, output) VALUES
                ('ASSESSMENT', 'p1', 1, 'first', 'ref-1', NULL, NULL, NULL),
                ('ASSESSMENT', 'p2', 1, 'second', NULL, NULL, NULL, NULL),
                ('ASSESSMENT', 'p3', 3, 'broken', NULL, NULL, CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'boom'),
                ('ASSESSMENT', 'p4', 4, 'waiting', NULL, NULL, NULL, NULL),
                ('ASSESSMENT', 'p5', 6, 'done', NULL, NULL, CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'ok'),
                ('ASSESSMENT', 'p6', 1, 'later', NULL, (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + interval '1 day', NULL, NULL),
                ('ASSESSMENT', 'p7', 0, 'was running', NULL, NULL, NULL, NULL),
                ('Notify', 'n1', 1, 'hello', NULL, NULL, NULL, NULL)
            """);
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @Test
    void unfinished_tasks_carry_over_in_order_with_their_state(PostgresDatabase database) throws Exception {
        legacyTable(database);

        LegacyImport.Report report = LegacyImport.run(database.dataSource(), LegacyImport.Options.defaults());

        assertThat(report.unfinished()).containsEntry("assessment", 6L).containsEntry("notify", 1L);
        assertThat(report.finished()).containsEntry("assessment", 0L);
        assertThat(database.rows("""
            SELECT t.identifier, q.state::text, t.payload, t.reference, q.run_at IS NOT NULL
            FROM pgtasks.task t JOIN pgtasks.queue q ON q.task_id = t.id JOIN pgtasks.topic tp ON tp.id = t.topic_id
            WHERE tp.name = 'assessment' ORDER BY t.id
            """)).containsExactly(
            row("p1", "ready", "first", "ref-1", false),
            row("p2", "ready", "second", null, false),
            row("p3", "failed", "broken", null, false),
            row("p4", "suspended", "waiting", null, false),
            row("p6", "scheduled", "later", null, true),
            row("p7", "ready", "was running", null, false));
        assertThat(database.rows("SELECT error->>'type' FROM pgtasks.task WHERE identifier = 'p3'")).containsExactly(List.of("task1.failed"));

        // The store claims them in the 1.x order.
        PostgresTaskStore store = new PostgresTaskStore(database.dataSource(), SchemaMode.NONE);
        try (StoreTransaction transaction = store.begin()) {
            int topic = store.findTopic(transaction, "assessment").orElseThrow();
            int node = store.registerNode(transaction, "importer-test", null);
            List<TaskStore.ClaimedTask> claimed = store.claim(transaction, new TaskStore.ClaimRequest(topic, node, Ordering.FIFO, 10, null)).tasks();
            assertThat(claimed).extracting(TaskStore.ClaimedTask::identifier).containsExactly("p1", "p2", "p7");
            transaction.rollback();
        }

        assertThat(LegacyImport.run(database.dataSource(), LegacyImport.Options.defaults()).skipped())
            .as("a second run leaves imported topics alone").containsExactly("assessment", "notify");
    }

    @Test
    void history_comes_along_when_asked_for(PostgresDatabase database) throws Exception {
        legacyTable(database);

        LegacyImport.Report report = LegacyImport.run(database.dataSource(), LegacyImport.Options.defaults().withHistory());

        assertThat(report.finished()).containsEntry("assessment", 1L);
        assertThat(database.rows("SELECT outcome::text, output FROM pgtasks.task WHERE identifier = 'p5'")).containsExactly(List.of("succeeded", "ok"));
    }

    @Test
    void imports_into_a_schema_named_otherwise(PostgresDatabase database) throws Exception {
        legacyTable(database);
        TaskSchema.migrate(database.dataSource(), "billing_tasks");

        LegacyImport.Report report = LegacyImport.run(database.dataSource(), LegacyImport.Options.defaults().withHistory().withSchema("billing_tasks"));

        assertThat(report.total()).isEqualTo(8);
        assertThat(database.count("SELECT count(*) FROM billing_tasks.task")).isEqualTo(8);
        assertThat(database.count("SELECT count(*) FROM billing_tasks.queue")).isEqualTo(7);
        assertThat(database.count("SELECT count(*) FROM pgtasks.task")).as("the default schema is left alone").isZero();
        assertThat(LegacyImport.run(database.dataSource(), LegacyImport.Options.defaults().withSchema("billing_tasks")).skipped())
            .containsExactly("assessment", "notify");
    }

    @Test
    void topic_names_that_would_clash_stop_the_import_before_anything_is_copied(PostgresDatabase database) throws Exception {
        legacyTable(database);

        assertThatThrownBy(() -> LegacyImport.run(database.dataSource(), LegacyImport.Options.defaults().withTopicNames(legacy -> "all")))
            .hasMessageContaining("would both become topic all");
        assertThat(database.count("SELECT count(*) FROM pgtasks.task")).isZero();
    }
}
