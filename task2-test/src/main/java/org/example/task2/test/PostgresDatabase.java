package org.example.task2.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * A fresh database on the shared test server. Closing it closes every pool it handed out and
 * drops the database.
 */
public final class PostgresDatabase implements AutoCloseable {

    private final TestPostgres.Server server;
    private final String name;
    private final List<HikariDataSource> pools = new CopyOnWriteArrayList<>();
    private volatile HikariDataSource dataSource;

    PostgresDatabase(TestPostgres.Server server, String name) {
        this.server = server;
        this.name = name;
    }

    public String name() {
        return name;
    }

    public String jdbcUrl() {
        return server.urlFor(name);
    }

    public String user() {
        return server.user();
    }

    public String password() {
        return server.password();
    }

    /** The default pool for this database, created on first use. */
    public DataSource dataSource() {
        HikariDataSource current = dataSource;
        if (current == null) {
            synchronized (this) {
                current = dataSource;
                if (current == null) {
                    current = dataSource = newPool("default", 10);
                }
            }
        }
        return current;
    }

    /** A separate pool, for example to stand in for a second pod. */
    public HikariDataSource newPool(String label, int size) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl());
        config.setUsername(user());
        config.setPassword(password());
        config.setMaximumPoolSize(size);
        config.setMinimumIdle(0);
        config.setPoolName(name + "-" + label);
        HikariDataSource pool = new HikariDataSource(config);
        pools.add(pool);
        return pool;
    }

    /** An unpooled connection in auto-commit mode. */
    public Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(), user(), password());
    }

    public void execute(String sql) {
        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("Could not execute: " + sql, e);
        }
    }

    /** Runs a query and returns each row as a list of column values. */
    public List<List<Object>> rows(String sql, Object... parameters) {
        try (Connection connection = connect(); PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int index = 0; index < parameters.length; index++) {
                statement.setObject(index + 1, parameters[index]);
            }
            List<List<Object>> rows = new ArrayList<>();
            try (ResultSet resultSet = statement.executeQuery()) {
                int columns = resultSet.getMetaData().getColumnCount();
                while (resultSet.next()) {
                    List<Object> row = new ArrayList<>(columns);
                    for (int column = 1; column <= columns; column++) {
                        row.add(resultSet.getObject(column));
                    }
                    rows.add(row);
                }
            }
            return rows;
        } catch (SQLException e) {
            throw new IllegalStateException("Could not query: " + sql, e);
        }
    }

    /** Runs a query that returns a single long, such as a count. */
    public long count(String sql, Object... parameters) {
        List<List<Object>> rows = rows(sql, parameters);
        if (rows.size() != 1 || rows.getFirst().size() != 1) {
            throw new IllegalStateException("Expected a single value from: " + sql);
        }
        return ((Number) rows.getFirst().getFirst()).longValue();
    }

    /** Closes every pool this database handed out, but keeps the database. */
    void closePools() {
        pools.forEach(HikariDataSource::close);
        pools.clear();
        dataSource = null;
    }

    @Override
    public void close() {
        pools.forEach(HikariDataSource::close);
        try (Connection connection = server.admin(); Statement statement = connection.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)");
        } catch (SQLException e) {
            throw new IllegalStateException("Could not drop test database " + name, e);
        }
    }
}
