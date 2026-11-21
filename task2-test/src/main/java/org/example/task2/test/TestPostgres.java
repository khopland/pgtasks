package org.example.task2.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private static final Map<String, String> TEMPLATES = new ConcurrentHashMap<>();

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

    /**
     * A database copied from a template that is prepared once per JVM, for a setup too slow to
     * repeat for every test. The copy has everything the template had, statistics included.
     *
     * @param template    names the template; the first call with a name prepares it
     * @param preparation fills the template; its connections are closed before it is copied
     */
    public static PostgresDatabase createDatabase(String template, PostgresExtension.Preparation preparation) {
        String source = TEMPLATES.computeIfAbsent(template, ignored -> {
            PostgresDatabase prepared = createDatabase();
            try {
                preparation.prepare(prepared);
            } catch (Exception e) {
                prepared.close();
                throw new IllegalStateException("Could not prepare template database " + template, e);
            }
            prepared.closePools();
            Runtime.getRuntime().addShutdownHook(new Thread(prepared::close, "task2-test-template-cleanup"));
            return prepared.name();
        });
        Server server = server();
        String name = "t" + ProcessHandle.current().pid() + "_" + COUNTER.incrementAndGet();
        try (Connection connection = server.admin(); Statement statement = connection.createStatement()) {
            // A template can't be copied while anyone is connected to it, and closed connections linger briefly.
            for (int attempt = 1; ; attempt++) {
                statement.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '" + source + "' AND pid <> pg_backend_pid()");
                try {
                    statement.execute("CREATE DATABASE " + name + " TEMPLATE " + source);
                    return new PostgresDatabase(server, name);
                } catch (SQLException e) {
                    if (!"55006".equals(e.getSQLState()) || attempt == 50) {
                        throw e;
                    }
                    Thread.sleep(20);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not create test database " + name + " from " + source, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while creating test database " + name, e);
        }
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
