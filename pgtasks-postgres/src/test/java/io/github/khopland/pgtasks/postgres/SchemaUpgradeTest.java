package io.github.khopland.pgtasks.postgres;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.admin.TaskQuery;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;
import io.github.khopland.pgtasks.spi.TaskStore.ClaimedTask;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;
import io.github.khopland.pgtasks.test.TestPostgres;
import io.github.khopland.pgtasks.test.tck.PushTck;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A database a released version left behind, with tasks in it, upgraded to the current files: the
 * upgrade must apply, keep every task usable, and leave a schema the store works on. Every versioned
 * file after the release's is covered as soon as it is in {@link TaskSchema#FILES}: V002 adds the
 * cancelled outcome, which the test uses on an upgraded task.
 *
 * <p>{@code released/<version>} in the test resources holds each release's files as it shipped them.
 * Add a directory, and a seed for it, when a release changes the schema. Refresh 0.1.0's R__ files
 * if they change before it is tagged; its V001 is frozen ({@link SchemaFreezeTest}).
 */
class SchemaUpgradeTest {

    /** What each release shipped, in the order its runner applied them. */
    private static final Map<String, List<String>> RELEASED_FILES = Map.of(
        "0.1.0", List.of(
            "V001__tasks_schema.sql",
            "R__create_topic.sql",
            "R__notify_limit.sql",
            "R__notify_ready.sql",
            "R__notify_topic.sql",
            "R__task_view.sql"
        )
    );

    /** Tasks in every state, as 0.1.0 stores them, written in SQL so they don't depend on the current store. */
    private static final String SEED_0_1_0 = """
        SELECT ${pgtasks_schema}.create_topic('from-0.1.0');
        INSERT INTO ${pgtasks_schema}.node (name, version) VALUES ('old-node', '0.1.0');
        WITH t AS (
            INSERT INTO ${pgtasks_schema}.task (topic_id, identifier, payload, payload_type, reference, headers)
            SELECT tp.id, 'person-' || g, 'payload-' || g, 'text', 'ref-' || g, '{"traceparent": "00-trace-span-01"}'::jsonb
            FROM ${pgtasks_schema}.topic tp, generate_series(1, 3) AS g
            WHERE tp.name = 'from-0.1.0'
            ORDER BY g
            RETURNING id, topic_id, identifier
        )
        INSERT INTO ${pgtasks_schema}.queue (task_id, topic_id, identifier, state)
        SELECT id, topic_id, identifier, 'ready' FROM t;
        WITH t AS (
            INSERT INTO ${pgtasks_schema}.task (topic_id, identifier, payload, error, attempts)
            SELECT id, 'person-9', 'broken', '{"type": "java.lang.IllegalStateException", "message": "boom"}', 3
            FROM ${pgtasks_schema}.topic WHERE name = 'from-0.1.0'
            RETURNING id, topic_id, identifier
        )
        INSERT INTO ${pgtasks_schema}.queue (task_id, topic_id, identifier, state, attempt, failures, first_failed_at)
        SELECT id, topic_id, identifier, 'failed', 3, 3, now() - interval '1 hour' FROM t;
        WITH t AS (
            INSERT INTO ${pgtasks_schema}.task (topic_id, identifier, payload)
            SELECT id, 'person-8', 'later' FROM ${pgtasks_schema}.topic WHERE name = 'from-0.1.0'
            RETURNING id, topic_id, identifier
        )
        INSERT INTO ${pgtasks_schema}.queue (task_id, topic_id, identifier, state, run_at)
        SELECT id, topic_id, identifier, 'scheduled', now() + interval '1 day' FROM t;
        INSERT INTO ${pgtasks_schema}.task (topic_id, identifier, payload, outcome, output, attempts, completed_at)
        SELECT id, 'person-0', 'done', 'succeeded', 'ok', 1, now() - interval '1 day' FROM ${pgtasks_schema}.topic WHERE name = 'from-0.1.0';
        INSERT INTO ${pgtasks_schema}.rate_admission (name, tokens, refilled_at) VALUES ('old-limit', 5, now());
        INSERT INTO ${pgtasks_schema}.rate_limit (name, rate, burst, updated_by) VALUES ('old-limit', 10, 5, 'operator');
        INSERT INTO ${pgtasks_schema}.schedule (topic_id, name, identifier, cron, zone, insertion, next_run_at)
        SELECT id, 'nightly', 'nightly', '0 0 3 * * *', 'UTC', 'APPEND', now() + interval '1 day' FROM ${pgtasks_schema}.topic WHERE name = 'from-0.1.0';
        """;

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.empty();

    static List<SchemaScript> released(String release) {
        List<SchemaScript> scripts = new ArrayList<>();
        for (String name : RELEASED_FILES.get(release)) {
            String path = "io/github/khopland/pgtasks/postgres/released/" + release + "/" + name;
            try (InputStream input = SchemaUpgradeTest.class.getClassLoader().getResourceAsStream(path)) {
                assertThat(input).as(path).isNotNull();
                scripts.add(SchemaScript.of(name, new String(input.readAllBytes(), StandardCharsets.UTF_8)));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return scripts;
    }

    /** The database as 0.1.0 left it: its files applied by the runner, with its tasks. */
    private static void at010(PostgresDatabase database) throws SQLException {
        try (Connection connection = database.connect()) {
            new SchemaMigrator(released("0.1.0"), TaskSchema.DEFAULT_SCHEMA).migrate(connection);
        }
        database.execute(TaskSchema.render(SEED_0_1_0, TaskSchema.DEFAULT_SCHEMA));
    }

    /** A new 0.1.0 database upgraded to the current files. Building one is quicker than copying a template. */
    private static PostgresDatabase upgraded() {
        PostgresDatabase database = TestPostgres.createDatabase();
        try {
            at010(database);
            TaskSchema.migrate(database.dataSource());
            return database;
        } catch (SQLException | RuntimeException e) {
            database.close();
            throw new IllegalStateException("Could not upgrade a 0.1.0 database", e);
        }
    }

    @Test
    void upgrades_a_0_1_0_database_and_keeps_its_tasks(PostgresDatabase database) throws SQLException {
        at010(database);
        List<String> changed = new ArrayList<>();
        List<SchemaScript> old = released("0.1.0");
        for (SchemaScript script : TaskSchema.scripts()) {
            boolean shipped = old.stream().anyMatch(each -> each.name().equals(script.name()) && each.checksum().equals(script.checksum()));
            if (!shipped) {
                changed.add(script.name());
            }
        }

        TaskSchema.Status status = TaskSchema.migrate(database.dataSource());

        assertThat(status.version()).isEqualTo(TaskSchema.VERSION);
        assertThat(status.applied()).as("new versioned files and changed repeatable ones").containsExactlyInAnyOrderElementsOf(changed);

        PostgresTaskStore store = new PostgresTaskStore(database.dataSource(), SchemaMode.VALIDATE);
        store.prepare();
        try (StoreTransaction transaction = store.begin()) {
            int topic = store.findTopic(transaction, "from-0.1.0").orElseThrow();
            int node = store.registerNode(transaction, "new-node", "current");

            List<ClaimedTask> claimed = store.claim(transaction, new TaskStore.ClaimRequest(topic, node, Ordering.FIFO, 10)).tasks();
            assertThat(claimed).extracting(ClaimedTask::identifier).containsExactly("person-1", "person-2", "person-3");
            assertThat(claimed.getFirst().payload()).isEqualTo("payload-1");
            assertThat(claimed.getFirst().reference()).isEqualTo("ref-1");
            assertThat(claimed.getFirst().headers()).containsEntry("traceparent", "00-trace-span-01");
            store.complete(transaction, topic, node, claimed.stream().map(task -> StoreFixture.completion(task, StoreFixture.succeeded())).toList());
            assertThat(store.count(transaction, topic).ready()).isZero();

            List<TaskView> deadLetters = store.deadLetters(transaction, topic, 0, 10);
            assertThat(deadLetters).extracting(TaskView::identifier).containsExactly("person-9");
            assertThat(deadLetters.getFirst().error()).contains("java.lang.IllegalStateException");
            assertThat(store.revive(transaction, topic, List.of(deadLetters.getFirst().id()), 10)).hasSize(1);

            long finished = database.count("SELECT id FROM pgtasks.task WHERE identifier = 'person-0'");
            assertThat(store.find(transaction, finished)).hasValueSatisfying(view -> {
                assertThat(view.state()).isEqualTo("succeeded");
                assertThat(view.output()).isEqualTo("ok");
            });
            // Rate limits are gone from the code; their 0.1.0 tables stay, unread, until a schema version drops them.
            assertThat(database.count("SELECT count(*) FROM pgtasks.rate_limit")).isOne();
            assertThat(store.schedules(transaction)).extracting(schedule -> schedule.name()).containsExactly("nightly");
            assertThat(store.promoteDue(transaction, 100).tasks()).as("the delayed task stays delayed").isZero();

            List<Long> cancelled = store.cancel(transaction, topic, TaskQuery.ALL.withIdentifier("person-8"), 10);
            assertThat(cancelled).as("V002's outcome works on a task 0.1.0 stored").hasSize(1);
            assertThat(store.find(transaction, cancelled.getFirst())).hasValueSatisfying(view -> assertThat(view.state()).isEqualTo("cancelled"));
            transaction.commit();
        }
        assertThat(database.rows("SELECT version, compatible_from FROM pgtasks.meta")).as("0.1.0 nodes may keep running against it")
            .containsExactly(List.of(TaskSchema.VERSION, 1));
        assertThat(TaskSchema.migrate(database.dataSource()).applied()).as("nothing left to apply").isEmpty();
    }

    /** The store's fixture on an upgraded database, which goes when the test ends. */
    private static StoreFixture upgradedFixture() {
        PostgresDatabase database = upgraded();
        return new PostgresStoreFixture(database, database.dataSource(), TaskSchema.DEFAULT_SCHEMA) {
            @Override
            public void close() {
                database.close();
            }
        };
    }

    /** A small part of the TCK on an upgraded database; the test above claims, completes and revives the upgraded tasks. */
    @Nested
    class Push extends PushTck {

        @Override
        protected StoreFixture createFixture() {
            return upgradedFixture();
        }
    }
}
