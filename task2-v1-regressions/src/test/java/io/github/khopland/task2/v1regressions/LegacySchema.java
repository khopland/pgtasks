package io.github.khopland.task2.v1regressions;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc.JdbcTaskRepository;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskDispatcher;
import io.github.khopland.task2.test.PostgresExtension;

/** The unmodified 1.x schema on a fresh Postgres database, plus a plain JDBC dispatcher. */
final class LegacySchema {

    private LegacySchema() {
    }

    static PostgresExtension extension() {
        return PostgresExtension.migrated(database -> {
            try (Connection connection = database.connect()) {
                @SuppressWarnings("deprecation")
                Liquibase liquibase = new Liquibase(
                    JdbcTaskRepository.CHANGE_LOG,
                    new ClassLoaderResourceAccessor(),
                    DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection))
                );
                liquibase.update(new Contexts());
            }
        });
    }

    /** Runs each dispatched function in its own committed transaction. */
    static TaskDispatcher<Connection, SQLException> dispatcher(DataSource dataSource) {
        return new TaskDispatcher<>() {
            @Override
            public <P> P apply(TransactionFunction<Connection, SQLException, P> function) throws SQLException {
                try (Connection connection = dataSource.getConnection()) {
                    connection.setAutoCommit(false);
                    try {
                        P payload = function.apply(connection);
                        connection.commit();
                        return payload;
                    } catch (Throwable throwable) {
                        connection.rollback();
                        throw throwable;
                    }
                }
            }
        };
    }

    static TaskSource.Order fifo() {
        return TaskSource.Order.FIRST_IN_FIRST_OUT;
    }
}
