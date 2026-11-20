package org.example.task2.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

/**
 * One real Postgres server per JVM, shared by every test.
 *
 * <p>By default this is an embedded server (zonky), which needs no Docker and no root. Set
 * {@code task2.test.postgres.url} (plus {@code .user} and {@code .password}) to an admin URL
 * to run against an existing server instead. Each test gets its own database, see
 * {@link #createDatabase()}.
 *
 * <p>{@code task2.test.postgres.config} adds settings to the embedded server, as
 * {@code key=value} pairs separated by semicolons, for instance to log slow plans:
 * {@code -Dtask2.test.postgres.config="shared_preload_libraries=auto_explain;auto_explain.log_min_duration=10ms"}.
 */
public final class TestPostgres {

    private static final AtomicInteger COUNTER = new AtomicInteger();

    private static volatile Server server;

    private TestPostgres() {
    }

    /** Creates an empty database with a unique name on the shared server. */
    public static PostgresDatabase createDatabase() {
        Server server = server();
        String name = "t" + ProcessHandle.current().pid() + "_" + COUNTER.incrementAndGet();
        try (Connection connection = server.admin(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + name);
        } catch (SQLException e) {
            throw new IllegalStateException("Could not create test database " + name, e);
        }
        return new PostgresDatabase(server, name);
    }

    static Server server() {
        Server current = server;
        if (current == null) {
            synchronized (TestPostgres.class) {
                current = server;
                if (current == null) {
                    current = server = startServer();
                }
            }
        }
        return current;
    }

    private static Server startServer() {
        String url = System.getProperty("task2.test.postgres.url");
        if (url != null && !url.isBlank()) {
            return new Server(
                url,
                System.getProperty("task2.test.postgres.user", "postgres"),
                System.getProperty("task2.test.postgres.password", "")
            );
        }
        try {
            EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder()
                .setServerConfig("max_connections", "400")
                .setServerConfig("fsync", "off")
                .setServerConfig("synchronous_commit", "off")
                .setServerConfig("full_page_writes", "off");
            for (String setting : System.getProperty("task2.test.postgres.config", "").split(";")) {
                int equals = setting.indexOf('=');
                if (equals > 0) {
                    builder.setServerConfig(setting.substring(0, equals).strip(), setting.substring(equals + 1).strip());
                }
            }
            EmbeddedPostgres embedded = builder.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    embedded.close();
                } catch (IOException ignored) {
                }
            }, "task2-test-postgres-shutdown"));
            return new Server("jdbc:postgresql://localhost:" + embedded.getPort() + "/postgres", "postgres", "");
        } catch (IOException e) {
            throw new UncheckedIOException("Could not start embedded Postgres", e);
        }
    }

    record Server(String adminUrl, String user, String password) {

        Connection admin() throws SQLException {
            return DriverManager.getConnection(adminUrl, user, password);
        }

        String urlFor(String database) {
            int query = adminUrl.indexOf('?');
            String base = query < 0 ? adminUrl : adminUrl.substring(0, query);
            String suffix = query < 0 ? "" : adminUrl.substring(query);
            return base.substring(0, base.lastIndexOf('/') + 1) + database + suffix;
        }
    }
}
