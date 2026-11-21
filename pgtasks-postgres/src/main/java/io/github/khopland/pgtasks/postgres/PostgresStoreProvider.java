package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import javax.sql.DataSource;

import io.github.khopland.pgtasks.spi.StoreProvider;
import io.github.khopland.pgtasks.spi.TaskStore;

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
    public OptionalInt poolSize(DataSource dataSource) {
        return PoolInfo.maximumSize(dataSource);
    }
}
