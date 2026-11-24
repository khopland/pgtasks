package io.github.khopland.pgtasks.postgres;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import io.github.khopland.pgtasks.SchemaMode;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The built-in runner and the version check, on empty databases. */
class SchemaMigratorTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.empty();

    /** The version a future release would add; the tests add files for it on top of the shipped ones. */
    private static final int NEXT = TaskSchema.VERSION + 1;

    private static final String DESCRIPTION = """
        ALTER TABLE pgtasks.topic ADD COLUMN description text;
        INSERT INTO pgtasks.meta (singleton, version, compatible_from) VALUES (true, %d, %d)
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
            return new SchemaMigrator(scripts, TaskSchema.DEFAULT_SCHEMA).migrate(connection);
        }
    }

    private static long history(PostgresDatabase database) {
        return database.count("SELECT count(*) FROM pgtasks.schema_history");
    }

    @Test
    void refuses_a_server_older_than_postgres_17() {
        assertThatThrownBy(() -> TaskSchema.checkServerVersion(160_013, "16.13"))
            .isInstanceOf(TaskSchemaException.class)
            .hasMessage("This database runs Postgres 16.13, but pgtasks needs Postgres 17 or later: its statements are planned for those versions. "
                + "Upgrade the server.");
        TaskSchema.checkServerVersion(170_000, "17.0");
        TaskSchema.checkServerVersion(190_000, "19.0");
    }

    @Test
    void applies_everything_once_and_then_nothing(PostgresDatabase database) throws SQLException {
        TaskSchema.Status first = TaskSchema.migrate(database.dataSource());
        TaskSchema.Status second = TaskSchema.migrate(database.dataSource());

        assertThat(first.applied()).containsExactlyElementsOf(TaskSchema.FILES);
        assertThat(first.version()).isEqualTo(TaskSchema.VERSION);
        assertThat(second.applied()).isEmpty();
        assertThat(history(database)).isEqualTo(TaskSchema.FILES.size());
        assertThat(database.rows("SELECT applied_by FROM pgtasks.schema_history").getFirst().getFirst()).isEqualTo(database.user());
    }

    @Test
    void the_library_version_matches_its_newest_file() {
        assertThat(new SchemaMigrator(shipped(), TaskSchema.DEFAULT_SCHEMA).latest()).isEqualTo(TaskSchema.VERSION);
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
                Pattern marker = Pattern.compile("INSERT INTO \\$\\{pgtasks_schema\\}\\.meta \\(singleton, version, compatible_from\\) VALUES \\(true, " + script.version() + ", \\d+\\)\\s+"
                    + "ON CONFLICT \\(singleton\\) DO UPDATE SET [^;]+;\\s*$");
                assertThat(marker.matcher(script.content()).find()).as(script.name() + " ends by recording its version").isTrue();
            }
            assertThat(script.transactional()).as(script.name() + " runs in a transaction").isTrue();
        }
    }

    @Test
    void the_liquibase_wrapper_lists_every_file_with_matching_flags() throws IOException {
        String changelog = Files.readString(Path.of("src/main/resources", TaskSchema.LIQUIBASE_CHANGELOG));
        Pattern changeSet = Pattern.compile("<changeSet id=\"\\$\\{pgtasks_schema\\}-([^\"]+)\" author=\"pgtasks\"([^>]*)>\\s*<sqlFile path=\"([^\"]+)\"([^>]*)/>");
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
        assertThat(database.rows("SELECT version, compatible_from FROM pgtasks.meta")).containsExactly(List.of(NEXT, 1));
    }

    @Test
    void a_failing_file_leaves_nothing_behind(PostgresDatabase database) throws SQLException {
        TaskSchema.migrate(database.dataSource());
        SchemaScript broken = SchemaScript.of(next("broken"), "ALTER TABLE pgtasks.topic ADD COLUMN half_done text;\nSELECT no_such_function();");

        assertThatThrownBy(() -> migrate(database, with(broken)))
            .isInstanceOf(TaskSchemaException.class)
            .hasMessageContaining("Could not apply " + next("broken") + ": statement 2 of 2 failed");

        assertThat(database.count("SELECT count(*) FROM information_schema.columns WHERE table_name = 'topic' AND column_name = 'half_done'")).isZero();
        assertThat(history(database)).isEqualTo(TaskSchema.FILES.size());
    }

    @Test
    void a_versioned_file_must_record_its_version(PostgresDatabase database) throws SQLException {
        TaskSchema.migrate(database.dataSource());

        assertThatThrownBy(() -> migrate(database, with(SchemaScript.of(next("forgot_meta"), "ALTER TABLE pgtasks.topic ADD COLUMN x text;"))))
            .isInstanceOf(TaskSchemaException.class)
            .hasMessageContaining(next("forgot_meta") + " must end by recording version " + NEXT + " in pgtasks.meta");
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

    private static SchemaScript shipped(String name) {
        return shipped().stream().filter(script -> script.name().equals(name)).findFirst().orElseThrow();
    }

    /** A shipped repeatable file with its revision raised by one, the way the next release would change it. */
    private static String nextRevision(String name, String from, String to) {
        SchemaScript script = shipped(name);
        String content = script.content();
        assertThat(content).contains(from);
        return content.replace("-- revision: " + script.revision() + "\n", "-- revision: " + (script.revision() + 1) + "\n").replace(from, to);
    }

    private static String changedView() {
        return nextRevision("R__task_view.sql", "t.headers\nFROM", "t.headers,\n       t.completed_at AS finished_at\nFROM");
    }

    @Test
    void reapplies_a_repeatable_file_when_it_changes(PostgresDatabase database) throws SQLException {
        TaskSchema.migrate(database.dataSource());
        String changed = changedView();
        assertThat(SchemaScript.of("R__task_view.sql", changed).revision()).isEqualTo(shipped("R__task_view.sql").revision() + 1);

        TaskSchema.Status status = migrate(database, replacing("R__task_view.sql", changed));

        assertThat(status.applied()).containsExactly("R__task_view.sql");
        assertThat(database.count("SELECT count(*) FROM information_schema.columns WHERE table_name = 'task_view' AND column_name = 'finished_at'")).isEqualTo(1);
        assertThat(migrate(database, replacing("R__task_view.sql", changed)).applied()).isEmpty();
        assertThat(database.rows("SELECT revision FROM pgtasks.schema_history WHERE script = 'R__task_view.sql' ORDER BY installed_rank"))
            .containsExactly(List.of(1), List.of(2));
    }

    @Test
    void an_older_node_does_not_roll_back_a_newer_repeatable_revision(PostgresDatabase database) throws SQLException {
        // A newer release changed the view without a new versioned file, and one of its nodes applied it.
        TaskSchema.migrate(database.dataSource());
        migrate(database, replacing("R__task_view.sql", changedView()));

        // An older node restarts mid-deploy: same schema version, older view.
        TaskSchema.Status status = TaskSchema.migrate(database.dataSource());

        assertThat(status.applied()).as("nothing rolled back").isEmpty();
        assertThat(database.count("SELECT count(*) FROM information_schema.columns WHERE table_name = 'task_view' AND column_name = 'finished_at'")).isEqualTo(1);
        assertThat(migrate(database, replacing("R__task_view.sql", changedView())).applied()).as("the newer node has nothing to restore").isEmpty();
    }

    @Test
    void a_newer_revision_replaces_what_an_older_runner_recorded(PostgresDatabase database) throws SQLException {
        // Rows an older runner recorded have no revision: they count as 0, so the current files replace them.
        TaskSchema.migrate(database.dataSource());
        database.execute("UPDATE pgtasks.schema_history SET revision = NULL, checksum = 'older' WHERE version IS NULL");

        assertThat(TaskSchema.migrate(database.dataSource()).applied()).containsExactlyElementsOf(
            TaskSchema.FILES.stream().filter(name -> name.startsWith("R__")).toList());
    }

    private static final String PUSH = """
        WITH t AS (
            INSERT INTO pgtasks.task (topic_id, identifier, payload)
            SELECT id, 'someone', 'payload' FROM pgtasks.topic WHERE name = 'busy'
            RETURNING id, topic_id, identifier
        )
        INSERT INTO pgtasks.queue (task_id, topic_id, identifier, state)
        SELECT id, topic_id, identifier, 'ready' FROM t
        """;

    /** A transaction that pushed and stays open, the way a long application transaction does: it holds a lock on the queue. */
    private static Connection openPush(PostgresDatabase database, Connection connection) throws SQLException {
        database.execute("SELECT pgtasks.create_topic('busy')");
        connection.setAutoCommit(false);
        try (var statement = connection.createStatement()) {
            statement.execute(PUSH);
        }
        return connection;
    }

    private static long pid(Connection connection) throws SQLException {
        try (var statement = connection.createStatement(); var resultSet = statement.executeQuery("SELECT pg_backend_pid()")) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    /** The trigger file with its revision raised: applying it takes a SHARE ROW EXCLUSIVE lock on the queue. */
    private static List<SchemaScript> changedTrigger() {
        return replacing("R__notify_ready.sql", nextRevision("R__notify_ready.sql", "Repeatable.", "Repeatable, revised."));
    }

    /** Pushes one task at a time until stopped, and returns the longest a push took, in ms. */
    private static CompletableFuture<Long> pushUntil(PostgresDatabase database, CompletableFuture<?> done) {
        return CompletableFuture.supplyAsync(() -> {
            long longest = 0;
            int pushes = 0;
            try (Connection connection = database.connect(); var statement = connection.createStatement()) {
                while (!done.isDone() || pushes == 0) {
                    long started = System.nanoTime();
                    statement.execute(PUSH);
                    longest = Math.max(longest, (System.nanoTime() - started) / 1_000_000);
                    pushes++;
                    Thread.sleep(10);
                }
            } catch (SQLException | InterruptedException e) {
                throw new IllegalStateException(e);
            }
            return longest;
        });
    }

    @Test
    void a_change_that_cannot_get_its_lock_gives_up_without_stalling_pushes(PostgresDatabase database) throws Exception {
        TaskSchema.migrate(database.dataSource());
        try (Connection blocker = database.connect()) {
            long blockerPid = pid(blocker);
            openPush(database, blocker);
            CompletableFuture<TaskSchema.Status> migration = CompletableFuture.supplyAsync(() -> {
                try (Connection connection = database.connect()) {
                    return new SchemaMigrator(changedTrigger(), TaskSchema.DEFAULT_SCHEMA, Duration.ofMillis(300), 3, Duration.ofMillis(100))
                        .migrate(connection);
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            });
            CompletableFuture<Long> pushes = pushUntil(database, migration);

            assertThatThrownBy(() -> migration.get(30, TimeUnit.SECONDS))
                .hasCauseInstanceOf(TaskSchemaException.class)
                .cause()
                .hasMessageContaining("Could not apply R__notify_ready.sql to the pgtasks schema: it waited more than 300 ms for a lock on each of 3 attempts")
                .hasMessageContaining("pid " + blockerPid + " (user " + database.user())
                .hasMessageContaining("idle in transaction")
                .hasMessageContaining("last statement: WITH t AS ( INSERT INTO pgtasks.task");
            assertThat(pushes.get(30, TimeUnit.SECONDS)).as("longest push, ms, while the change waited").isLessThan(2_000);
            blocker.rollback();
        }
        assertThat(database.rows("SELECT revision FROM pgtasks.schema_history WHERE script = 'R__notify_ready.sql'")).containsExactly(List.of(1));
        assertThat(database.rows("SELECT current_setting('lock_timeout')")).as("on other sessions").containsExactly(List.of("0"));
    }

    @Test
    void a_change_that_times_out_on_a_lock_is_applied_once_the_lock_is_free(PostgresDatabase database) throws Exception {
        TaskSchema.migrate(database.dataSource());
        Connection blocker = openPush(database, database.connect());
        CompletableFuture.delayedExecutor(700, TimeUnit.MILLISECONDS).execute(() -> {
            try (blocker) {
                blocker.commit();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        TaskSchema.Status status;
        String lockTimeout;
        try (Connection connection = database.connect()) {
            status = new SchemaMigrator(changedTrigger(), TaskSchema.DEFAULT_SCHEMA, Duration.ofMillis(200), 10, Duration.ofMillis(100))
                .migrate(connection);
            try (var statement = connection.createStatement(); var resultSet = statement.executeQuery("SELECT current_setting('lock_timeout')")) {
                resultSet.next();
                lockTimeout = resultSet.getString(1);
            }
        }

        assertThat(status.applied()).containsExactly("R__notify_ready.sql");
        assertThat(database.rows("SELECT revision FROM pgtasks.schema_history WHERE script = 'R__notify_ready.sql' ORDER BY installed_rank"))
            .containsExactly(List.of(1), List.of(2));
        assertThat(lockTimeout).as("the connection's own setting, restored").isEqualTo("0");
    }

    @Test
    void reads_the_revision_from_the_header_only() {
        assertThat(SchemaScript.of("R__x.sql", "-- About x.\n-- revision: 7\nSELECT 1;").revision()).isEqualTo(7);
        assertThat(SchemaScript.of("R__x.sql", "SELECT 1;\n-- revision: 7\n").revision()).isZero();
        assertThat(SchemaScript.of("R__x.sql", "SELECT 1;").revision()).isZero();
    }

    @Test
    void runs_marked_files_outside_a_transaction(PostgresDatabase database) throws SQLException {
        TaskSchema.migrate(database.dataSource());
        SchemaScript concurrently = SchemaScript.of(next("index_concurrently"), """
            -- Builds an index without blocking writers.
            %s
            CREATE INDEX CONCURRENTLY IF NOT EXISTS topic_created_idx ON pgtasks.topic (created_at);
            INSERT INTO pgtasks.meta (singleton, version, compatible_from) VALUES (true, %d, 1)
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
        String newerView = "CREATE OR REPLACE VIEW pgtasks.task_view AS SELECT 'newer' AS marker;";
        migrate(database, with(description(1)).stream()
            .map(script -> script.name().equals("R__task_view.sql") ? SchemaScript.of(script.name(), "DROP VIEW IF EXISTS pgtasks.task_view;\n" + newerView) : script)
            .toList());

        TaskSchema.Status status = TaskSchema.migrate(database.dataSource());

        assertThat(status.version()).isEqualTo(NEXT);
        assertThat(status.expected()).isEqualTo(TaskSchema.VERSION);
        assertThat(status.applied()).as("nothing downgraded").isEmpty();
        assertThat(database.rows("SELECT marker FROM pgtasks.task_view")).containsExactly(List.of("newer"));
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
        database.execute("UPDATE pgtasks.meta SET version = " + (NEXT + 1) + ", compatible_from = 1");

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
            assertThatThrownBy(() -> TaskSchema.check(TaskSchema.DEFAULT_SCHEMA, TaskSchema.readMeta(connection, TaskSchema.DEFAULT_SCHEMA), NEXT, List.of()))
                .isInstanceOf(TaskSchemaException.class)
                .hasMessageContaining("is at version " + TaskSchema.VERSION + ", but this library needs version " + NEXT);
        }
    }

    @Test
    void a_schema_applied_by_another_tool_is_not_migrated_over(PostgresDatabase database) throws SQLException {
        // What a DBA pipeline does: run the files in order, with no runner history.
        try (Connection connection = database.connect(); var statement = connection.createStatement()) {
            for (SchemaScript script : shipped()) {
                statement.execute(script.content(TaskSchema.DEFAULT_SCHEMA));
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
        assertThat(database.count("SELECT count(*) FROM pg_namespace WHERE nspname = 'pgtasks'")).isZero();

        new PostgresTaskStore(database.dataSource()).prepare();
        new PostgresTaskStore(database.dataSource(), SchemaMode.VALIDATE).prepare();
        assertThat(history(database)).isEqualTo(TaskSchema.FILES.size());
    }
}
