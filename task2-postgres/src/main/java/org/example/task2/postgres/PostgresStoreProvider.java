package org.example.task2.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;

import javax.sql.DataSource;

import org.example.task2.spi.StoreProvider;
import org.example.task2.spi.TaskStore;

/** Lets {@code TaskEngine.builder().dataSource(ds)} find the Postgres store. */
public final class PostgresStoreProvider implements StoreProvider {

    @Override
    public boolean supports(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            return connection.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT).contains("postgres");
        } catch (SQLException e) {
            throw new IllegalStateException("Could not inspect the DataSource", e);
        }
    }

    @Override
    public TaskStore create(DataSource dataSource, Map<String, String> options) {
        String mode = options.getOrDefault("schema-mode", "migrate");
        return new PostgresTaskStore(dataSource, SchemaMode.valueOf(mode.toUpperCase(Locale.ROOT).replace('-', '_')));
    }

    @Override
    public java.util.OptionalInt poolSize(DataSource dataSource) {
        return PoolInfo.maximumSize(dataSource);
    }
}
