package org.example.task2.postgres;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import javax.sql.DataSource;

/**
 * What a connection pool says about itself, read without depending on it. Knows HikariCP, the
 * pool Spring Boot uses by default: its size, and how to connect without it.
 */
final class PoolInfo {

    private static final String HIKARI = "com.zaxxer.hikari.HikariDataSource";

    private PoolInfo() {
    }

    /** The pool's maximum size, if the DataSource is a pool that says. */
    static OptionalInt maximumSize(DataSource dataSource) {
        return hikari(dataSource).map(pool -> {
            Object size = call(pool, "getMaximumPoolSize");
            return size instanceof Integer value && value > 0 ? OptionalInt.of(value) : OptionalInt.empty();
        }).orElse(OptionalInt.empty());
    }

    /** How to open a connection of one's own, bypassing the pool: a JDBC URL and its properties. */
    record Direct(String url, Properties properties) {
    }

    static Optional<Direct> direct(DataSource dataSource) {
        return hikari(dataSource).flatMap(pool -> {
            if (!(call(pool, "getJdbcUrl") instanceof String url) || url.isBlank()) {
                return Optional.empty();
            }
            Properties properties = new Properties();
            if (call(pool, "getDataSourceProperties") instanceof Properties extra) {
                properties.putAll(extra);
            }
            if (call(pool, "getUsername") instanceof String user) {
                properties.setProperty("user", user);
            }
            if (call(pool, "getPassword") instanceof String password) {
                properties.setProperty("password", password);
            }
            return Optional.of(new Direct(url, properties));
        });
    }

    /** Whether the DataSource opens a new physical connection each time, so holding one costs no pool slot. */
    static boolean unpooled(DataSource dataSource) {
        String name = dataSource.getClass().getName();
        return name.equals("org.postgresql.ds.PGSimpleDataSource")
            || name.equals("org.springframework.jdbc.datasource.DriverManagerDataSource")
            || name.equals("org.springframework.jdbc.datasource.SimpleDriverDataSource");
    }

    private static Optional<Object> hikari(DataSource dataSource) {
        try {
            Class<?> type = Class.forName(HIKARI, false, dataSource.getClass().getClassLoader());
            return dataSource.isWrapperFor(type) ? Optional.of(dataSource.unwrap(type)) : Optional.empty();
        } catch (ClassNotFoundException | SQLException | LinkageError e) {
            return Optional.empty();
        }
    }

    private static Object call(Object target, String method) {
        try {
            Method getter = target.getClass().getMethod(method);
            return getter.invoke(target);
        } catch (ReflectiveOperationException | RuntimeException e) {
            return null;
        }
    }
}
