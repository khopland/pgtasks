package org.example.task2.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.example.task2.Insertion;
import org.example.task2.spi.StoreTransaction;
import org.example.task2.test.tck.StoreFixture;
import org.postgresql.ds.PGSimpleDataSource;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SchemaTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresStoreFixture.extension();

    @Test
    void creates_every_table_and_records_its_version(PostgresDatabase database) {
        assertThat(database.count("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'tasks' AND table_name IN ('queue', 'task', 'topic', 'node', 'attempt', 'rate_limit', 'rate_admission', 'schedule', 'topic_count', 'lease', 'meta', 'schema_history')"))
            .isEqualTo(12);
        assertThat(database.rows("SELECT version, compatible_from FROM tasks.meta")).containsExactly(List.of(1, 1));
        assertThat(database.rows("SELECT version, script FROM tasks.schema_history ORDER BY installed_rank")).containsExactly(
            Arrays.asList(1, "V001__tasks_schema.sql"),
            Arrays.asList(null, "R__create_topic.sql"),
            Arrays.asList(null, "R__notify_limit.sql"),
            Arrays.asList(null, "R__notify_ready.sql"),
            Arrays.asList(null, "R__notify_topic.sql"),
            Arrays.asList(null, "R__task_view.sql")
        );
    }

    @Test
    void uses_enums_and_timestamptz(PostgresDatabase database) {
        assertThat(database.rows("SELECT enum_range(NULL::tasks.queue_state)::text").getFirst().getFirst())
            .isEqualTo("{scheduled,retrying,ready,active,suspended,failed}");
        assertThat(database.rows("SELECT enum_range(NULL::tasks.task_outcome)::text").getFirst().getFirst())
            .isEqualTo("{succeeded,filtered,failed,redundant,recreated}");
        assertThat(database.count("SELECT count(*) FROM information_schema.columns WHERE table_schema = 'tasks' AND data_type = 'timestamp without time zone'"))
            .isZero();
    }

    @Test
    void create_topic_is_idempotent_and_only_adds_a_row(PostgresDatabase database) throws SQLException {
        PostgresStoreFixture fixture = new PostgresStoreFixture(database);
        long relations = database.count("SELECT count(*) FROM pg_class WHERE relnamespace = 'tasks'::regnamespace");
        int first = fixture.topic("assessment");
        int again = fixture.topic("assessment");
        int other = fixture.topic("notify");

        assertThat(again).isEqualTo(first);
        assertThat(other).as("registering an existing topic uses no identity value").isEqualTo(first + 1);
        assertThat(database.count("SELECT count(*) FROM pg_class WHERE relnamespace = 'tasks'::regnamespace")).as("no new tables").isEqualTo(relations);
        assertThat(database.count("SELECT count(*) FROM pg_partitioned_table p JOIN pg_class c ON c.oid = p.partrelid WHERE c.relnamespace = 'tasks'::regnamespace"))
            .as("nothing is partitioned").isZero();
    }

    @Test
    void indexes_on_the_task_table_can_be_built_concurrently(PostgresDatabase database) {
        database.execute("CREATE INDEX CONCURRENTLY task_payload_type_idx ON tasks.task (payload_type)");
        database.execute("DROP INDEX CONCURRENTLY tasks.task_payload_type_idx");
    }

    @Test
    void indexes_from_the_plan_exist(PostgresDatabase database) {
        assertThat(database.rows("SELECT indexname FROM pg_indexes WHERE schemaname = 'tasks' AND tablename IN ('queue', 'task') ORDER BY indexname"))
            .extracting(row -> (String) row.getFirst())
            .contains(
                "queue_ready_idx", "queue_identifier_idx", "queue_due_idx", "queue_active_idx", "queue_dedup_idx",
                "task_identifier_idx", "task_reference_idx", "task_root_idx", "task_created_brin", "task_completed_idx"
            );
        assertThat((String) database.rows("SELECT indexdef FROM pg_indexes WHERE indexname = 'queue_ready_idx'").getFirst().getFirst())
            .contains("(topic_id, priority DESC, task_id) WHERE (state = 'ready'");
    }

    @Test
    void a_runtime_user_without_ddl_rights_can_create_topics_and_push(PostgresDatabase database) throws SQLException {
        database.execute("CREATE ROLE task_runtime LOGIN PASSWORD 'runtime'");
        database.execute("GRANT USAGE ON SCHEMA tasks TO task_runtime");
        database.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA tasks TO task_runtime");
        database.execute("GRANT USAGE ON ALL SEQUENCES IN SCHEMA tasks TO task_runtime");
        database.execute("GRANT EXECUTE ON FUNCTION tasks.create_topic(text, text, jsonb) TO task_runtime");
        PGSimpleDataSource runtimeUser = new PGSimpleDataSource();
        runtimeUser.setURL(database.jdbcUrl());
        runtimeUser.setUser("task_runtime");
        runtimeUser.setPassword("runtime");
        PostgresTaskStore store = new PostgresTaskStore(runtimeUser, SchemaMode.VALIDATE);
        try (Connection runtime = runtimeUser.getConnection()) {
            assertThatThrownBy(() -> runtime.createStatement().execute("CREATE TABLE tasks.sneaky (id int)"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("permission denied");

            store.prepare();
            try (StoreTransaction transaction = store.begin()) {
                int topic = store.createTopic(transaction, "created-at-runtime");
                assertThat(store.push(transaction, topic, Insertion.APPEND, List.of(StoreFixture.task("person-1")))).hasSize(1);
                transaction.commit();
            }
        } finally {
            database.execute("DROP OWNED BY task_runtime");
            database.execute("DROP ROLE task_runtime");
        }
    }

    @Test
    void create_topic_is_not_executable_by_public(PostgresDatabase database) {
        assertThat(database.count("SELECT count(*) FROM information_schema.role_routine_grants WHERE routine_schema = 'tasks' AND routine_name = 'create_topic' AND grantee = 'PUBLIC'"))
            .isZero();
        assertThat(database.rows("SELECT prosecdef, proconfig::text FROM pg_proc WHERE proname = 'create_topic'").getFirst())
            .as("runs with the caller's rights and a fixed search_path")
            .containsExactly(false, "{\"search_path=pg_catalog, pg_temp\"}");
    }
}
