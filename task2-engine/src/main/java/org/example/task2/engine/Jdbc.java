package org.example.task2.engine;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/** Runs engine work in short transactions of its own. */
final class Jdbc {

    private final DataSource dataSource;

    Jdbc(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @FunctionalInterface
    interface Work<T> {
        T run(Connection connection) throws Exception;
    }

    <T> T transact(Work<T> work) throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = work.run(connection);
                connection.commit();
                return result;
            } catch (Exception | Error e) {
                try {
                    connection.rollback();
                } catch (SQLException rollback) {
                    e.addSuppressed(rollback);
                }
                throw e;
            }
        }
    }
}
