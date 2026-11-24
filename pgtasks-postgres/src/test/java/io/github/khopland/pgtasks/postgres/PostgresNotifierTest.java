package io.github.khopland.pgtasks.postgres;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.github.khopland.pgtasks.spi.Notifier;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/** The notifier's connection: how it is set up, and how it recovers when it is lost. */
class PostgresNotifierTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));

    private static final String APPLICATION = "pgtasks-notifier-test";

    private PostgresDatabase database;
    private final Recorder heard = new Recorder();
    private final List<AutoCloseable> resources = new CopyOnWriteArrayList<>();

    @BeforeEach
    void database(PostgresDatabase database) {
        this.database = database;
    }

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable resource : resources.reversed()) {
            resource.close();
        }
    }

    @Test
    void listens_on_a_pooled_connection_that_starts_with_auto_commit_off() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(database.jdbcUrl());
        config.setUsername(database.user());
        config.setPassword(database.password());
        config.setMaximumPoolSize(2);
        config.setAutoCommit(false);
        HikariDataSource pool = new HikariDataSource(config);
        resources.add(pool);

        PostgresNotifier notifier = start(new PostgresNotifier(pool::getConnection));

        database.execute("NOTIFY pgtasks_ready, '7'");
        assertThat(heard.events.poll(5, TimeUnit.SECONDS)).isEqualTo("ready 7");
        assertThat(database.count("SELECT count(*) FROM pg_stat_activity WHERE datname = ? AND state = 'idle in transaction'",
            database.name())).isZero();
        assertThat(notifier.connected()).isTrue();
    }

    @Test
    void a_message_on_a_channel_arrives_once_committed_and_only_on_channels_it_listens_to() throws Exception {
        start(new PostgresNotifier(database::connect));
        PostgresTaskStore store = new PostgresTaskStore(database.dataSource());
        try (StoreTransaction transaction = store.begin()) {
            store.notify(transaction, "custom", "rolled back");
            transaction.rollback();
        }
        try (StoreTransaction transaction = store.begin()) {
            store.notify(transaction, "other", "not listened to");
            store.notify(transaction, "custom", "hello");
            transaction.commit();
        }

        assertThat(heard.events.poll(5, TimeUnit.SECONDS)).isEqualTo("custom hello");
        assertThat(heard.events.poll(300, TimeUnit.MILLISECONDS)).isNull();
    }

    @Test
    void reconnects_when_the_backend_is_terminated() throws Exception {
        String url = withApplicationName(database.jdbcUrl());
        PostgresNotifier notifier = start(new PostgresNotifier(() -> DriverManager.getConnection(url, database.user(), database.password())));

        assertThat(database.rows("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = ?", APPLICATION))
            .hasSize(1);

        assertThat(heard.events.poll(10, TimeUnit.SECONDS)).as("everything polled once reconnected").isEqualTo("missed");
        awaitConnected(notifier);
        database.execute("NOTIFY pgtasks_ready, '8'");
        assertThat(heard.events.poll(5, TimeUnit.SECONDS)).isEqualTo("ready 8");
    }

    /** A connection the network dropped without closing: nothing arrives, and nothing fails until something is sent. */
    @Test
    void reconnects_when_the_connection_goes_quiet_without_closing() throws Exception {
        URI target = URI.create(database.jdbcUrl().substring("jdbc:".length()));
        Proxy proxy = new Proxy(target.getHost(), target.getPort());
        resources.add(proxy);
        String url = database.jdbcUrl().replace(target.getHost() + ":" + target.getPort(), "127.0.0.1:" + proxy.port());
        PostgresNotifier notifier = start(new PostgresNotifier(() -> DriverManager.getConnection(url, database.user(), database.password()),
            TaskSchema.DEFAULT_SCHEMA, Duration.ofMillis(200), Duration.ofSeconds(1)));

        proxy.blackholeOpenLinks();
        database.execute("NOTIFY pgtasks_ready, '9'");
        assertThat(heard.events.poll(500, TimeUnit.MILLISECONDS)).as("lost on the way").isNull();

        assertThat(heard.events.poll(10, TimeUnit.SECONDS)).as("everything polled once reconnected").isEqualTo("missed");
        awaitConnected(notifier);
        database.execute("NOTIFY pgtasks_ready, '10'");
        assertThat(heard.events.poll(5, TimeUnit.SECONDS)).isEqualTo("ready 10");
    }

    private PostgresNotifier start(PostgresNotifier notifier) throws InterruptedException {
        resources.add(notifier::close);
        notifier.start(heard, Set.of(Notifier.TOPIC, "custom"));
        awaitConnected(notifier);
        Thread.sleep(200); // LISTEN has run
        return notifier;
    }

    private static String withApplicationName(String url) {
        return url + (url.contains("?") ? "&" : "?") + "ApplicationName=" + APPLICATION;
    }

    private static void awaitConnected(Notifier notifier) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!notifier.connected() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(notifier.connected()).isTrue();
    }

    private static final class Recorder implements Notifier.Listener {

        final BlockingQueue<String> events = new LinkedBlockingQueue<>();

        @Override
        public void ready(int topicId) {
            events.add("ready " + topicId);
        }

        @Override
        public void notified(String channel, String payload) {
            events.add(channel + " " + payload);
        }

        @Override
        public void missed() {
            events.add("missed");
        }
    }

    /** Forwards TCP to the database, and can stop forwarding on the links it has without closing them. */
    private static final class Proxy implements AutoCloseable {

        private final ServerSocket server;
        private final List<Link> links = new CopyOnWriteArrayList<>();

        Proxy(String host, int port) throws IOException {
            server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread.ofVirtual().start(() -> {
                while (!server.isClosed()) {
                    try {
                        Socket client = server.accept();
                        Link link = new Link(client, new Socket(host, port));
                        links.add(link);
                        link.start();
                    } catch (IOException e) {
                        return;
                    }
                }
            });
        }

        int port() {
            return server.getLocalPort();
        }

        void blackholeOpenLinks() {
            links.forEach(link -> link.blackholed = true);
        }

        @Override
        public void close() throws IOException {
            server.close();
            for (Link link : links) {
                link.client.close();
                link.upstream.close();
            }
        }
    }

    private static final class Link {

        final Socket client;
        final Socket upstream;
        volatile boolean blackholed;

        Link(Socket client, Socket upstream) {
            this.client = client;
            this.upstream = upstream;
        }

        void start() {
            Thread.ofVirtual().start(() -> pump(client, upstream));
            Thread.ofVirtual().start(() -> pump(upstream, client));
        }

        private void pump(Socket from, Socket to) {
            byte[] buffer = new byte[8192];
            try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                int read;
                while ((read = in.read(buffer)) >= 0) {
                    if (!blackholed) {
                        out.write(buffer, 0, read);
                        out.flush();
                    }
                }
            } catch (IOException ignored) {
                // closed
            }
        }
    }
}
