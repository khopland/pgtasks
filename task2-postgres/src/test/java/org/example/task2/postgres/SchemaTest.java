package org.example.task2.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.example.task2.Insertion;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SchemaTest {

    @RegisterExtension
    static PostgresExtension postgres = StoreFixture.extension();

    @Test
    void creates_every_table_and_records_its_version(PostgresDatabase database) {
        assertThat(database.count("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'tasks' AND table_name IN ('queue', 'task', 'topic', 'node', 'attempt', 'rate_limit', 'rate_admission', 'schedule', 'meta', 'schema_history')"))
            .isEqualTo(10);
        assertThat(database.rows("SELECT version, compatible_from FROM tasks.meta")).containsExactly(List.of(1, 1));
        assertThat(database.rows("SELECT version, script FROM tasks.schema_history ORDER BY installed_rank")).containsExactly(
            Arrays.asList(1, "V001__tasks_schema.sql"),
            Arrays.asList(null, "R__create_topic.sql"),
            Arrays.asList(null, "R__notify_ready.sql"),
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
    void create_topic_is_idempotent_and_creates_a_partition(PostgresDatabase database) throws SQLException {
        StoreFixture fixture = new StoreFixture(database);
        int first = fixture.topic("assessment");
        int again = fixture.topic("assessment");
        int other = fixture.topic("notify");

        assertThat(again).isEqualTo(first);
        assertThat(other).isNotEqualTo(first);
        assertThat(database.rows("SELECT relname FROM pg_inherits JOIN pg_class ON oid = inhrelid WHERE inhparent = 'tasks.task'::regclass ORDER BY relname"))
            .containsExactly(List.of("task_" + first), List.of("task_" + other));
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
        PostgresTaskStore store = new PostgresTaskStore();
        try (Connection runtime = DriverManager.getConnection(database.jdbcUrl(), "task_runtime", "runtime")) {
            assertThatThrownBy(() -> runtime.createStatement().execute("CREATE TABLE tasks.sneaky (id int)"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("permission denied");

            int topic = store.createTopic(runtime, "created-at-runtime");
            assertThat(store.push(runtime, topic, Insertion.APPEND, List.of(StoreFixture.task("person-1")))).hasSize(1);
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
            .containsExactly(true, "{\"search_path=pg_catalog, pg_temp\"}");
    }
}
