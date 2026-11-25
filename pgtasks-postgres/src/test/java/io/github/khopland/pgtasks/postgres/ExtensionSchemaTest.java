package io.github.khopland.pgtasks.postgres;

import java.sql.SQLException;
import java.util.List;

import io.github.khopland.pgtasks.SchemaMode;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** An extension's SQL files stay in its jar and use a history table of their own. */
final class ExtensionSchemaTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.empty();

    private static final ExtensionSchema WIDGET = ExtensionSchema.of("widget", ExtensionSchemaTest.class,
        "io/github/khopland/pgtasks/postgres/test-db",
        List.of("V001__widget.sql", "R__widget_view.sql"), List.of("widget"));

    @Test
    void applies_its_files_once_and_keeps_its_own_history(PostgresDatabase database) throws SQLException {
        assertThat(WIDGET.migrate(database.dataSource(), TaskSchema.DEFAULT_SCHEMA)).isEqualTo(1);

        assertThat(database.count("SELECT count(*) FROM pgtasks.widget")).isZero();
        assertThat(database.rows("SELECT to_regclass('pgtasks.widget_view')").getFirst().getFirst()).isNotNull();
        assertThat(database.rows("SELECT script FROM pgtasks.widget_schema_history ORDER BY installed_rank"))
            .extracting(row -> row.get(0)).containsExactly("V001__widget.sql", "R__widget_view.sql");
        assertThat(database.rows("SELECT to_regclass('pgtasks.schema_history')").getFirst().getFirst()).isNull();

        assertThat(WIDGET.migrate(database.dataSource(), TaskSchema.DEFAULT_SCHEMA)).isEqualTo(1);
        assertThat(database.count("SELECT count(*) FROM pgtasks.widget_schema_history")).isEqualTo(2);
    }

    @Test
    void validate_needs_the_extension_tables_to_be_applied(PostgresDatabase database) throws SQLException {
        assertThatThrownBy(() -> WIDGET.validate(database.dataSource(), TaskSchema.DEFAULT_SCHEMA))
            .isInstanceOf(TaskSchemaException.class)
            .hasMessageContaining("pgtasks.widget table is missing")
            .hasMessageContaining("classpath:io/github/khopland/pgtasks/postgres/test-db/V001__widget.sql");

        WIDGET.migrate(database.dataSource(), TaskSchema.DEFAULT_SCHEMA);
        WIDGET.validate(database.dataSource(), TaskSchema.DEFAULT_SCHEMA);
    }

    @Test
    void apply_follows_the_store_schema_mode(PostgresDatabase database) throws SQLException {
        WIDGET.apply(new PostgresTaskStore(database.dataSource(), SchemaMode.NONE, TaskSchema.DEFAULT_SCHEMA));
        assertThat(database.rows("SELECT to_regclass('pgtasks.widget')").getFirst().getFirst()).isNull();

        assertThatThrownBy(() -> WIDGET.apply(new PostgresTaskStore(database.dataSource(), SchemaMode.VALIDATE, TaskSchema.DEFAULT_SCHEMA)))
            .isInstanceOf(TaskSchemaException.class).hasMessageContaining("pgtasks.widget table is missing");

        WIDGET.apply(new PostgresTaskStore(database.dataSource(), SchemaMode.MIGRATE, TaskSchema.DEFAULT_SCHEMA));
        assertThat(database.rows("SELECT to_regclass('pgtasks.widget')").getFirst().getFirst()).hasToString("pgtasks.widget");
        WIDGET.apply(new PostgresTaskStore(database.dataSource(), SchemaMode.VALIDATE, TaskSchema.DEFAULT_SCHEMA));
    }

    @Test
    void the_engine_files_stay_free_of_extension_tables() {
        assertThat(TaskSchema.scripts()).noneMatch(script -> script.content().contains("rate_limit_window"));
    }
}
