package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.SchemaMode;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.ds.PGSimpleDataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Runs on the default schema and on one named otherwise. */
@ParameterizedClass(name = "schema {0}")
@ValueSource(strings = {TaskSchema.DEFAULT_SCHEMA, PostgresStoreFixture.CUSTOM_SCHEMA})
class SchemaTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.empty();

    @Parameter
    String schema;

    @BeforeEach
    void migrate(PostgresDatabase database) throws SQLException {
        TaskSchema.migrate(database.dataSource(), schema);
    }

    /** SQL written for the default schema, its name as a literal included, on this test's schema. */
    private String sql(String text) {
        return TaskSchema.qualify(text, schema).replace("'pgtasks'", "'" + schema + "'").replace("SCHEMA pgtasks ", "SCHEMA " + schema + " ");
    }

    @Test
    void only_the_configured_schema_exists(PostgresDatabase database) {
        assertThat(database.count("SELECT count(*) FROM pg_namespace WHERE nspname IN ('pgtasks', '" + PostgresStoreFixture.CUSTOM_SCHEMA + "')"))
            .isEqualTo(1);
        assertThat(database.count(sql("SELECT count(*) FROM pg_namespace WHERE nspname = 'pgtasks'"))).isEqualTo(1);
    }

    @Test
    void every_trigger_notifies_on_the_schemas_own_channels(PostgresDatabase database) {
        assertThat(database.rows(sql("SELECT p.proname FROM pg_proc p WHERE p.pronamespace = 'pgtasks'::regnamespace AND p.prosrc LIKE '%pg_notify%' ORDER BY 1")))
            .extracting(row -> (String) row.getFirst())
            .containsExactly("notify_ready", "notify_topic_delete", "notify_topic_insert");
        assertThat(database.rows(sql("SELECT p.prosrc FROM pg_proc p WHERE p.pronamespace = 'pgtasks'::regnamespace AND p.prosrc LIKE '%pg_notify%'")))
            .extracting(row -> (String) row.getFirst())
            .allSatisfy(source -> assertThat(source).containsPattern("pg_notify\\('" + schema + "_(ready|topic)'"));
    }

    @Test
    void creates_every_table_and_records_its_version(PostgresDatabase database) {
        assertThat(database.count(sql("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'pgtasks' AND table_name IN ('queue', 'task', 'topic', 'node', 'topic_count', 'lease', 'meta', 'schema_history')")))
            .isEqualTo(8);
        assertThat(database.rows(sql("SELECT version, compatible_from FROM pgtasks.meta"))).containsExactly(List.of(2, 1));
        assertThat(database.rows(sql("SELECT version, script FROM pgtasks.schema_history ORDER BY installed_rank"))).containsExactly(
            Arrays.asList(1, "V001__tasks_schema.sql"),
            Arrays.asList(2, "V002__cancelled_outcome.sql"),
            Arrays.asList(null, "R__create_topic.sql"),
            Arrays.asList(null, "R__notify_ready.sql"),
            Arrays.asList(null, "R__notify_topic.sql"),
            Arrays.asList(null, "R__task_view.sql")
        );
    }

    @Test
    void uses_enums_and_timestamptz(PostgresDatabase database) {
        assertThat(database.rows(sql("SELECT enum_range(NULL::pgtasks.queue_state)::text")).getFirst().getFirst())
            .isEqualTo("{scheduled,retrying,ready,active,suspended,dead_letter}");
        assertThat(database.rows(sql("SELECT enum_range(NULL::pgtasks.task_outcome)::text")).getFirst().getFirst())
            .isEqualTo("{succeeded,filtered,failed,redundant,recreated,cancelled}");
        assertThat(database.count(sql("SELECT count(*) FROM information_schema.columns WHERE table_schema = 'pgtasks' AND data_type = 'timestamp without time zone'")))
            .isZero();
    }

    @Test
    void create_topic_is_idempotent_and_only_adds_a_row(PostgresDatabase database) throws SQLException {
        PostgresStoreFixture fixture = new PostgresStoreFixture(database, database.dataSource(), schema);
        long relations = database.count(sql("SELECT count(*) FROM pg_class WHERE relnamespace = 'pgtasks'::regnamespace"));
        int first = fixture.topic("assessment");
        int again = fixture.topic("assessment");
        int other = fixture.topic("notify");

        assertThat(again).isEqualTo(first);
        assertThat(other).as("registering an existing topic uses no identity value").isEqualTo(first + 1);
        assertThat(database.count(sql("SELECT count(*) FROM pg_class WHERE relnamespace = 'pgtasks'::regnamespace"))).as("no new tables").isEqualTo(relations);
        assertThat(database.count(sql("SELECT count(*) FROM pg_partitioned_table p JOIN pg_class c ON c.oid = p.partrelid WHERE c.relnamespace = 'pgtasks'::regnamespace")))
            .as("nothing is partitioned").isZero();
    }

    @Test
    void indexes_on_the_task_table_can_be_built_concurrently(PostgresDatabase database) {
        database.execute(sql("CREATE INDEX CONCURRENTLY task_payload_type_idx ON pgtasks.task (payload_type)"));
        database.execute(sql("DROP INDEX CONCURRENTLY pgtasks.task_payload_type_idx"));
    }

    @Test
    void indexes_from_the_plan_exist(PostgresDatabase database) {
        assertThat(database.rows(sql("SELECT indexname FROM pg_indexes WHERE schemaname = 'pgtasks' AND tablename IN ('queue', 'task') ORDER BY indexname")))
            .extracting(row -> (String) row.getFirst())
            .contains(
                "queue_ready_idx", "queue_identifier_idx", "queue_due_idx", "queue_active_idx", "queue_dedup_idx",
                "task_identifier_idx", "task_reference_idx", "task_root_idx", "task_created_brin", "task_completed_idx"
            );
        assertThat((String) database.rows(sql("SELECT indexdef FROM pg_indexes WHERE indexname = 'queue_ready_idx'")).getFirst().getFirst())
            .contains("(topic_id, priority DESC, task_id) WHERE (state = 'ready'");
    }

    @Test
    void a_runtime_user_without_ddl_rights_can_create_topics_and_push(PostgresDatabase database) throws SQLException {
        database.execute(sql("CREATE ROLE task_runtime LOGIN PASSWORD 'runtime'"));
        database.execute(sql("GRANT USAGE ON SCHEMA pgtasks TO task_runtime"));
        database.execute(sql("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA pgtasks TO task_runtime"));
        database.execute(sql("GRANT USAGE ON ALL SEQUENCES IN SCHEMA pgtasks TO task_runtime"));
        database.execute(sql("GRANT EXECUTE ON FUNCTION pgtasks.create_topic(text) TO task_runtime"));
        PGSimpleDataSource runtimeUser = new PGSimpleDataSource();
        runtimeUser.setURL(database.jdbcUrl());
        runtimeUser.setUser("task_runtime");
        runtimeUser.setPassword("runtime");
        PostgresTaskStore store = new PostgresTaskStore(runtimeUser, SchemaMode.VALIDATE, schema);
        try (Connection runtime = runtimeUser.getConnection()) {
            assertThatThrownBy(() -> runtime.createStatement().execute(sql("CREATE TABLE pgtasks.sneaky (id int)")))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("permission denied");

            store.prepare();
            try (StoreTransaction transaction = store.begin()) {
                int topic = store.createTopic(transaction, "created-at-runtime");
                assertThat(store.push(transaction, topic, Insertion.APPEND, List.of(StoreFixture.task("person-1")))).hasSize(1);
                transaction.commit();
            }
        } finally {
            database.execute(sql("DROP OWNED BY task_runtime"));
            database.execute(sql("DROP ROLE task_runtime"));
        }
    }

    @Test
    void create_topic_is_not_executable_by_public(PostgresDatabase database) {
        assertThat(database.count(sql("SELECT count(*) FROM information_schema.role_routine_grants WHERE routine_schema = 'pgtasks' AND routine_name = 'create_topic' AND grantee = 'PUBLIC'")))
            .isZero();
        assertThat(database.rows(sql("SELECT prosecdef, proconfig::text FROM pg_proc WHERE proname = 'create_topic'")).getFirst())
            .as("runs with the caller's rights and a fixed search_path")
            .containsExactly(false, "{\"search_path=pg_catalog, pg_temp\"}");
    }
}
