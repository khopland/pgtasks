package io.github.khopland.pgtasks.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** The built-in runner and the version check, on empty databases. */
class SchemaMigratorTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.empty();

    /** The version a future release would add; the tests add files for it on top of the shipped ones. */
    private static final int NEXT = TaskSchema.VERSION + 1;

    private static final String DESCRIPTION = """
        ALTER TABLE tasks.topic ADD COLUMN description text;
        INSERT INTO tasks.meta (singleton, version, compatible_from) VALUES (true, %d, %d)
        ON CONFLICT (singleton) DO UPDATE SET version = excluded.version, compatible_from = excluded.compatible_from, updated_at = now();
        """;

    private static String next(String description) {
        return "V%03d__%s.sql".formatted(NEXT, description);
    }

    /** The next version's file, compatible with libraries from {@code compatibleFrom} on. */
    private static SchemaScript description(int compatibleFrom) {
        return SchemaScript.of(next("topic_description"), DESCRIPTION.formatted(NEXT, compatibleFrom));
    }

    private static List<SchemaScript> shipped() {
        return TaskSchema.scripts();
    }

    private static List<SchemaScript> with(SchemaScript... extra) {
        List<SchemaScript> scripts = new ArrayList<>(shipped());
        scripts.addAll(List.of(extra));
        return scripts;
    }

    private static List<SchemaScript> replacing(String name, String content) {
        return shipped().stream().map(script -> script.name().equals(name) ? SchemaScript.of(name, content) : script).toList();
    }

    private static TaskSchema.Status migrate(PostgresDatabase database, List<SchemaScript> scripts) throws SQLException {
        try (Connection connection = database.connect()) {
            return new SchemaMigrator(scripts).migrate(connection);
        }
    }

    private static long history(PostgresDatabase database) {
        return database.count("SELECT count(*) FROM tasks.schema_history");
    }

    @Test
    void applies_everything_once_and_then_nothing(PostgresDatabase database) throws SQLException {
        TaskSchema.Status first = TaskSchema.migrate(database.dataSource());
        TaskSchema.Status second = TaskSchema.migrate(database.dataSource());

        assertThat(first.applied()).containsExactlyElementsOf(TaskSchema.FILES);
        assertThat(first.version()).isEqualTo(TaskSchema.VERSION);
        assertThat(second.applied()).isEmpty();
        assertThat(history(database)).isEqualTo(TaskSchema.FILES.size());
        assertThat(database.rows("SELECT applied_by FROM tasks.schema_history").getFirst().getFirst()).isEqualTo(database.user());
    }

    @Test
    void the_library_version_matches_its_newest_file() {
        assertThat(new SchemaMigrator(shipped()).latest()).isEqualTo(TaskSchema.VERSION);
    }

    @Test
    void every_shipped_file_is_listed_and_follows_the_conventions() throws IOException {
        Path location = Path.of("src/main/resources", TaskSchema.LOCATION);
        try (Stream<Path> files = Files.list(location)) {
            assertThat(files.map(path -> path.getFileName().toString()).filter(name -> name.endsWith(".sql")).sorted().toList())
                .as("files on disk; list new ones in TaskSchema.FILES")
                .containsExactlyInAnyOrderElementsOf(TaskSchema.FILES);
        }
        for (SchemaScript script : shipped()) {
            if (!script.repeatable()) {
                Pattern marker = Pattern.compile("INSERT INTO tasks\\.meta \\(singleton, version, compatible_from\\) VALUES \\(true, " + script.version() + ", \\d+\\)\\s+"
                    + "ON CONFLICT \\(singleton\\) DO UPDATE SET [^;]+;\\s*$");
                assertThat(marker.matcher(script.content()).find()).as(script.name() + " ends by recording its version").isTrue();
            }
            assertThat(script.transactional()).as(script.name() + " runs in a transaction").isTrue();
        }
    }

    @Test
    void the_liquibase_wrapper_lists_every_file_with_matching_flags() throws IOException {
        String changelog = Files.readString(Path.of("src/main/resources", TaskSchema.LIQUIBASE_CHANGELOG));
        Pattern changeSet = Pattern.compile("<changeSet id=\"tasks-([^\"]+)\" author=\"pgtasks\"([^>]*)>\\s*<sqlFile path=\"([^\"]+)\"([^>]*)/>");
        var matcher = changeSet.matcher(changelog);
        List<String> listed = new ArrayList<>();
        while (matcher.find()) {
            SchemaScript script = shipped().stream().filter(each -> each.name().equals(matcher.group(3))).findFirst().orElseThrow();
            listed.add(script.name());
            assertThat(matcher.group(2).contains("runOnChange=\"true\"")).as(script.name() + " runOnChange").isEqualTo(script.repeatable());
            assertThat(matcher.group(2).contains("runInTransaction=\"false\"")).as(script.name() + " runInTransaction").isEqualTo(!script.transactional());
            assertThat(matcher.group(4)).contains("splitStatements=\"false\"");
        }
        assertThat(listed).as("in the order a DBA pipeline runs them").containsExactlyElementsOf(TaskSchema.FILES);
    }

    @Test
    void applies_a_new_versioned_file_in_its_own_transaction(PostgresDatabase database) throws SQLException {
        TaskSchema.migrate(database.dataSource());

        TaskSchema.Status status = migrate(database, with(description(1)));

        assertThat(status.applied()).containsExactly(next("topic_description"));
        assertThat(database.rows("SELECT version, compatible_from FROM tasks.meta")).containsExactly(List.of(NEXT, 1));
    }

    @Test
    void a_failing_file_leaves_nothing_behind(PostgresDatabase database) throws SQLException {
        TaskSchema.migrate(database.dataSource());
        SchemaScript broken = SchemaScript.of(next("broken"), "ALTER TABLE tasks.topic ADD COLUMN half_done text;\nSELECT no_such_function();");

        assertThatThrownBy(() -> migrate(database, with(broken)))
            .isInstanceOf(TaskSchemaException.class)
            .hasMessageContaining("Could not apply " + next("broken") + ": statement 2 of 2 failed");

        assertThat(database.count("SELECT count(*) FROM information_schema.columns WHERE table_name = 'topic' AND column_name = 'half_done'")).isZero();
        assertThat(history(database)).isEqualTo(TaskSchema.FILES.size());
    }

    @Test
    void a_versioned_file_must_record_its_version(PostgresDatabase database) throws SQLException {
        TaskSchema.migrate(database.dataSource());

        assertThatThrownBy(() -> migrate(database, with(SchemaScript.of(next("forgot_meta"), "ALTER TABLE tasks.topic ADD COLUMN x text;"))))
            .isInstanceOf(TaskSchemaException.class)
            .hasMessageContaining(next("forgot_meta") + " must end by recording version " + NEXT + " in tasks.meta");
        assertThat(database.count("SELECT count(*) FROM information_schema.columns WHERE table_name = 'topic' AND column_name = 'x'")).isZero();
    }

    @Test
    void refuses_to_run_when_an_applied_file_changed(PostgresDatabase database) throws SQLException {
        TaskSchema.migrate(database.dataSource());
        String changed = shipped().getFirst().content().replace("fillfactor = 80", "fillfactor = 70");

        assertThatThrownBy(() -> migrate(database, replacing("V001__tasks_schema.sql", changed)))
            .isInstanceOf(TaskSchemaException.class)
            .hasMessageContaining("V001__tasks_schema.sql has changed since it was applied")
            .hasMessageContaining("add a new versioned file instead");
    }

    @Test
    void reapplies_a_repeatable_file_when_it_changes(PostgresDatabase database) throws SQLException {
        TaskSchema.migrate(database.dataSource());
        String changed = shipped().stream().filter(script -> script.name().equals("R__task_view.sql")).findFirst().orElseThrow().content()
            .replace("t.error\nFROM", "t.error,\n       t.trace_context\nFROM");

        TaskSchema.Status status = migrate(database, replacing("R__task_view.sql", changed));

        assertThat(status.applied()).containsExactly("R__task_view.sql");
        assertThat(database.count("SELECT count(*) FROM information_schema.columns WHERE table_name = 'task_view' AND column_name = 'trace_context'")).isEqualTo(1);
        assertThat(migrate(database, replacing("R__task_view.sql", changed)).applied()).isEmpty();
    }

    @Test
    void runs_marked_files_outside_a_transaction(PostgresDatabase database) throws SQLException {
        TaskSchema.migrate(database.dataSource());
        SchemaScript concurrently = SchemaScript.of(next("index_concurrently"), """
            -- Builds an index without blocking writers.
            %s
            CREATE INDEX CONCURRENTLY IF NOT EXISTS topic_created_idx ON tasks.topic (created_at);
            INSERT INTO tasks.meta (singleton, version, compatible_from) VALUES (true, %d, 1)
            ON CONFLICT (singleton) DO UPDATE SET version = excluded.version, compatible_from = excluded.compatible_from, updated_at = now();
            """.formatted(SchemaScript.NO_TRANSACTION, NEXT));
        assertThat(concurrently.transactional()).isFalse();

        assertThat(migrate(database, with(concurrently)).applied()).containsExactly(next("index_concurrently"));
        assertThat(database.count("SELECT count(*) FROM pg_indexes WHERE indexname = 'topic_created_idx'")).isEqualTo(1);
    }

    @Test
    void concurrent_runners_apply_each_file_once(PostgresDatabase database) throws Exception {
        List<CompletableFuture<TaskSchema.Status>> runners = new ArrayList<>();
        for (int index = 0; index < 4; index++) {
            runners.add(CompletableFuture.supplyAsync(() -> {
                try {
                    return TaskSchema.migrate(database.dataSource());
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            }));
        }
        List<String> applied = new ArrayList<>();
        for (CompletableFuture<TaskSchema.Status> runner : runners) {
            applied.addAll(runner.get(30, TimeUnit.SECONDS).applied());
        }

        assertThat(applied).hasSameSizeAs(TaskSchema.FILES).doesNotHaveDuplicates();
        assertThat(history(database)).isEqualTo(TaskSchema.FILES.size());
    }

    @Test
    void an_older_node_leaves_a_newer_compatible_schema_alone(PostgresDatabase database) throws SQLException {
        String newerView = "CREATE OR REPLACE VIEW tasks.task_view AS SELECT 'newer' AS marker;";
        migrate(database, with(description(1)).stream()
            .map(script -> script.name().equals("R__task_view.sql") ? SchemaScript.of(script.name(), "DROP VIEW IF EXISTS tasks.task_view;\n" + newerView) : script)
            .toList());

        TaskSchema.Status status = TaskSchema.migrate(database.dataSource());

        assertThat(status.version()).isEqualTo(NEXT);
        assertThat(status.expected()).isEqualTo(TaskSchema.VERSION);
        assertThat(status.applied()).as("nothing downgraded").isEmpty();
        assertThat(database.rows("SELECT marker FROM tasks.task_view")).containsExactly(List.of("newer"));
        assertThat(TaskSchema.validate(database.dataSource()).version()).isEqualTo(NEXT);
    }

    @Test
    void an_older_node_refuses_a_newer_incompatible_schema(PostgresDatabase database) throws SQLException {
        migrate(database, with(description(NEXT)));

        assertThatThrownBy(() -> TaskSchema.migrate(database.dataSource()))
            .isInstanceOf(TaskSchemaException.class)
            .hasMessageContaining("only compatible with library schema version " + NEXT + " and later, but this library uses version " + TaskSchema.VERSION);
        assertThatThrownBy(() -> TaskSchema.validate(database.dataSource()))
            .isInstanceOf(TaskSchemaException.class)
            .hasMessageContaining("upgrade this application");
    }

    @Test
    void more_than_one_version_ahead_is_refused(PostgresDatabase database) throws SQLException {
        TaskSchema.migrate(database.dataSource());
        database.execute("UPDATE tasks.meta SET version = " + (NEXT + 1) + ", compatible_from = 1");

        assertThatThrownBy(() -> TaskSchema.validate(database.dataSource()))
            .isInstanceOf(TaskSchemaException.class)
            .hasMessageContaining("more than one version ahead");
    }

    @Test
    void validate_names_the_options_when_the_schema_is_missing_or_old(PostgresDatabase database) throws SQLException {
        assertThatThrownBy(() -> TaskSchema.validate(database.dataSource()))
            .isInstanceOf(TaskSchemaException.class)
            .hasMessageContaining("schema mode MIGRATE")
            .hasMessageContaining("Flyway, Liquibase or your DBA pipeline")
            .hasMessageContaining("classpath:" + TaskSchema.LOCATION);

        TaskSchema.migrate(database.dataSource());
        assertThat(TaskSchema.validate(database.dataSource()).version()).isEqualTo(TaskSchema.VERSION);

        try (Connection connection = database.connect()) {
            assertThatThrownBy(() -> TaskSchema.check(TaskSchema.readMeta(connection), NEXT, List.of()))
                .isInstanceOf(TaskSchemaException.class)
                .hasMessageContaining("is at version " + TaskSchema.VERSION + ", but this library needs version " + NEXT);
        }
    }

    @Test
    void a_schema_applied_by_another_tool_is_not_migrated_over(PostgresDatabase database) throws SQLException {
        // What a DBA pipeline does: run the files in order, with no runner history.
        try (Connection connection = database.connect(); var statement = connection.createStatement()) {
            for (SchemaScript script : shipped()) {
                statement.execute(script.content());
            }
        }
        assertThat(TaskSchema.validate(database.dataSource()).version()).isEqualTo(TaskSchema.VERSION);

        assertThatThrownBy(() -> TaskSchema.migrate(database.dataSource()))
            .isInstanceOf(TaskSchemaException.class)
            .hasMessageContaining("another tool applied it")
            .hasMessageContaining("schema mode VALIDATE");
    }

    @Test
    void the_store_prepares_the_schema_by_its_mode(PostgresDatabase database) throws SQLException {
        assertThatThrownBy(() -> new PostgresTaskStore(database.dataSource(), SchemaMode.VALIDATE).prepare()).isInstanceOf(TaskSchemaException.class);
        new PostgresTaskStore(database.dataSource(), SchemaMode.NONE).prepare();
        assertThat(database.count("SELECT count(*) FROM pg_namespace WHERE nspname = 'tasks'")).isZero();

        new PostgresTaskStore(database.dataSource()).prepare();
        new PostgresTaskStore(database.dataSource(), SchemaMode.VALIDATE).prepare();
        assertThat(history(database)).isEqualTo(TaskSchema.FILES.size());
    }
}
