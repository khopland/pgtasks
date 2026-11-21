package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.TestPostgres;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The same SQL files, applied four ways: the built-in runner, Flyway, the Liquibase wrapper and a
 * DBA pipeline running the files in order. Every path must produce the same tasks schema, apart
 * from its own history table, and pass the library's version check.
 */
class SchemaParityTest {

    private final List<PostgresDatabase> databases = new ArrayList<>();

    @AfterEach
    void dropDatabases() {
        databases.forEach(PostgresDatabase::close);
    }

    private PostgresDatabase database() {
        PostgresDatabase database = TestPostgres.createDatabase();
        databases.add(database);
        return database;
    }

    private static void runner(PostgresDatabase database) throws SQLException {
        TaskSchema.migrate(database.dataSource());
    }

    private static int flyway(PostgresDatabase database) {
        return Flyway.configure()
            .dataSource(database.dataSource())
            .locations("classpath:" + TaskSchema.LOCATION)
            .schemas(TaskSchema.SCHEMA)
            .table("flyway_task_history")
            .load()
            .migrate()
            .migrationsExecuted;
    }

    @SuppressWarnings("deprecation")
    private static void liquibase(PostgresDatabase database) throws Exception {
        try (Connection connection = database.connect()) {
            Liquibase liquibase = new Liquibase(TaskSchema.LIQUIBASE_CHANGELOG, new ClassLoaderResourceAccessor(),
                DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection)));
            liquibase.update(new Contexts());
        }
    }

    /** What psql -f does with each file, in order, as the schema owner. */
    private static void dba(PostgresDatabase database) throws SQLException {
        try (Connection connection = database.connect(); Statement statement = connection.createStatement()) {
            for (SchemaScript script : TaskSchema.scripts()) {
                statement.execute(script.content());
            }
        }
    }

    @Test
    void every_path_produces_the_same_schema() throws Exception {
        PostgresDatabase byRunner = database(), byFlyway = database(), byLiquibase = database(), byDba = database();
        runner(byRunner);
        assertThat(flyway(byFlyway)).isEqualTo(TaskSchema.FILES.size());
        liquibase(byLiquibase);
        dba(byDba);

        List<String> expected = catalog(byRunner);
        assertThat(expected).as("the catalog dump sees the schema").hasSizeGreaterThan(100);
        assertThat(catalog(byFlyway)).as("Flyway").containsExactlyElementsOf(expected);
        assertThat(catalog(byLiquibase)).as("Liquibase").containsExactlyElementsOf(expected);
        assertThat(catalog(byDba)).as("DBA pipeline").containsExactlyElementsOf(expected);

        for (PostgresDatabase database : List.of(byRunner, byFlyway, byLiquibase, byDba)) {
            assertThat(TaskSchema.validate(database.dataSource()).version()).isEqualTo(TaskSchema.VERSION);
        }
    }

    @Test
    void every_tool_applies_the_files_once() throws Exception {
        PostgresDatabase byRunner = database(), byFlyway = database(), byLiquibase = database();
        runner(byRunner);
        flyway(byFlyway);
        liquibase(byLiquibase);
        List<String> before = catalog(byRunner);

        assertThat(TaskSchema.migrate(byRunner.dataSource()).applied()).isEmpty();
        assertThat(flyway(byFlyway)).isZero();
        liquibase(byLiquibase);

        assertThat(catalog(byRunner)).isEqualTo(before);
        assertThat(catalog(byFlyway)).isEqualTo(before);
        assertThat(catalog(byLiquibase)).isEqualTo(before);
    }

    /** The tasks schema as sorted lines, without the migration tools' own history tables. */
    static List<String> catalog(PostgresDatabase database) {
        String own = "NOT (%s LIKE 'schema_history%%' OR %s LIKE 'flyway_task_history%%')";
        List<String> lines = new ArrayList<>();
        String[] queries = {
            """
            SELECT 'relation ' || c.relname || ' kind=' || c.relkind::text || ' options=' || coalesce(c.reloptions::text, '')
                   || ' partition=' || coalesce(pg_get_partkeydef(c.oid), '')
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'tasks' AND %s
            """.formatted(own.formatted("c.relname", "c.relname")),
            """
            SELECT 'column ' || c.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod)
                   || ' notnull=' || a.attnotnull || ' identity=' || a.attidentity::text
                   || ' default=' || coalesce(pg_get_expr(d.adbin, d.adrelid), '')
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE n.nspname = 'tasks' AND a.attnum > 0 AND NOT a.attisdropped AND c.relkind IN ('r', 'p', 'v') AND %s
            """.formatted(own.formatted("c.relname", "c.relname")),
            """
            SELECT 'constraint ' || c.relname || '.' || k.conname || ' ' || pg_get_constraintdef(k.oid)
            FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid JOIN pg_namespace n ON n.oid = k.connamespace
            WHERE n.nspname = 'tasks' AND %s
            """.formatted(own.formatted("c.relname", "c.relname")),
            """
            SELECT 'index ' || indexname || ' ' || indexdef FROM pg_indexes
            WHERE schemaname = 'tasks' AND %s
            """.formatted(own.formatted("tablename", "tablename")),
            """
            SELECT 'function ' || pg_get_functiondef(p.oid) || ' acl=' || coalesce(p.proacl::text, '')
            FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'tasks'
            """,
            """
            SELECT 'view ' || viewname || ' ' || definition FROM pg_views WHERE schemaname = 'tasks'
            """,
            """
            SELECT 'trigger ' || pg_get_triggerdef(t.oid)
            FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'tasks' AND NOT t.tgisinternal
            """,
            """
            SELECT 'type ' || t.typname || ' ' || string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder)
            FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid JOIN pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname = 'tasks'
            GROUP BY t.typname
            """,
            """
            SELECT 'meta ' || version || ' compatible from ' || compatible_from FROM tasks.meta
            """
        };
        for (String query : queries) {
            database.rows(query).forEach(row -> lines.add((String) row.getFirst()));
        }
        return lines.stream().sorted().toList();
    }
}
