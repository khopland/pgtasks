package org.example.task2.chaos;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTransientConnectionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.example.task2.test.PostgresDatabase;

/**
 * One node's connection pool, with switches for trouble: cut the database off, freeze the node,
 * or kill it. Every connection carries the node's name as its application name, so the node's
 * backends can be terminated on the server.
 */
final class ChaosDataSource implements DataSource, AutoCloseable {

    private enum Mode { NORMAL, CUT, FROZEN }

    private final PostgresDatabase database;
    private final String name;
    private final HikariDataSource pool;
    private final AtomicInteger refused = new AtomicInteger();
    private final Object gate = new Object();
    private volatile Mode mode = Mode.NORMAL;

    ChaosDataSource(PostgresDatabase database, String name) {
        this.database = database;
        this.name = name;
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(database.jdbcUrl());
        config.setUsername(database.user());
        config.setPassword(database.password());
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(2000);
        config.setPoolName(name);
        config.addDataSourceProperty("ApplicationName", name);
        pool = new HikariDataSource(config);
    }

    @Override
    public Connection getConnection() throws SQLException {
        while (true) {
            switch (mode) {
                case NORMAL -> {
                    return pool.getConnection();
                }
                case CUT -> {
                    refused.incrementAndGet();
                    throw new SQLTransientConnectionException("The network to the database is cut for " + name);
                }
                case FROZEN -> {
                    synchronized (gate) {
                        while (mode == Mode.FROZEN) {
                            try {
                                gate.wait();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new SQLTransientConnectionException("Interrupted while frozen", e);
                            }
                        }
                    }
                }
            }
        }
    }

    /** Refuses new connections and drops the open ones, like a network partition. */
    void cut() {
        mode = Mode.CUT;
        terminate();
    }

    /** Blocks every new connection, like a paused process: no heartbeats, no completions. */
    void freeze() {
        mode = Mode.FROZEN;
    }

    void restore() {
        synchronized (gate) {
            mode = Mode.NORMAL;
            gate.notifyAll();
        }
    }

    /** Terminates the node's backends on the server; its transactions roll back. */
    void terminate() {
        database.rows("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = ?", name);
    }

    int refused() {
        return refused.get();
    }

    @Override
    public void close() {
        restore();
        pool.close();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PrintWriter getLogWriter() {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
    }

    @Override
    public void setLoginTimeout(int seconds) {
    }

    @Override
    public int getLoginTimeout() {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        throw new SQLException("Not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> type) {
        return false;
    }

    @Override
    public String toString() {
        return name;
    }
}
