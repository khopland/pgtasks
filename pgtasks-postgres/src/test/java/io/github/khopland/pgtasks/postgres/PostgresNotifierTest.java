package io.github.khopland.pgtasks.postgres;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.github.khopland.pgtasks.spi.Notifier;
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

    private PostgresNotifier start(PostgresNotifier notifier) throws InterruptedException {
        resources.add(notifier::close);
        notifier.start(heard);
        awaitConnected(notifier);
        Thread.sleep(200); // LISTEN has run
        return notifier;
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
        public void missed() {
            events.add("missed");
        }
    }
}
