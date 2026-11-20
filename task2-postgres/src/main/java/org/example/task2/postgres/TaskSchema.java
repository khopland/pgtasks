package org.example.task2.postgres;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * The {@code tasks} schema. Migrate it at startup with {@link #migrate}, or run
 * {@link #CHANGELOG} from your own migration pipeline.
 *
 * <p>The runtime user needs USAGE on the schema, SELECT, INSERT, UPDATE and DELETE on its tables,
 * USAGE on {@code task_id_seq}, and EXECUTE on {@code create_topic}. It doesn't need DDL rights:
 * {@code create_topic} creates partitions as the schema owner.
 */
public final class TaskSchema {

    public static final String DEFAULT_SCHEMA = "tasks";

    public static final String CHANGELOG = "org/example/task2/postgres/changelog.xml";

    private TaskSchema() {
    }

    /**
     * Runs the changelog with Liquibase, which must be on the classpath. Liquibase's own tracking
     * tables live in the task schema, so several task schemas can share one database.
     */
    public static void migrate(DataSource dataSource) throws SQLException {
        migrate(dataSource, DEFAULT_SCHEMA);
    }

    public static void migrate(DataSource dataSource, String schema) throws SQLException {
        new PostgresTaskStore(schema); // validates the name
        try (Connection connection = dataSource.getConnection()) {
            try (var statement = connection.createStatement()) {
                statement.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
            }
            LiquibaseMigration.run(connection, schema);
        }
    }

    /** Kept apart so that TaskSchema loads without Liquibase on the classpath. */
    private static final class LiquibaseMigration {

        @SuppressWarnings("deprecation")
        static void run(Connection connection, String schema) throws SQLException {
            try {
                liquibase.database.Database database = liquibase.database.DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new liquibase.database.jvm.JdbcConnection(connection));
                database.setLiquibaseSchemaName(schema);
                liquibase.Liquibase liquibase = new liquibase.Liquibase(CHANGELOG, new liquibase.resource.ClassLoaderResourceAccessor(), database);
                liquibase.setChangeLogParameter("schema", schema);
                liquibase.update(new liquibase.Contexts());
            } catch (liquibase.exception.LiquibaseException e) {
                throw new SQLException("Could not migrate the " + schema + " schema", e);
            }
        }
    }
}
