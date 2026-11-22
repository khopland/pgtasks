package io.github.khopland.pgtasks.engine;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pushes the engine sees commit don't notify inside the transaction; the engine notifies after the
 * commit, debounced, and other nodes still wake at once. The consumers poll once an hour, so only
 * a notification can wake them in time.
 */
class WakeUpTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final TopicRef<String> TOPIC = TopicRef.of("woken", String.class);
    private static final TopicRef<String> UPSTREAM = TopicRef.of("upstream", String.class);

    private PostgresDatabase database;
    private EngineFixture fixture;
    private final CountDownLatch done = new CountDownLatch(1);

    @BeforeEach
    void setUp(PostgresDatabase database) throws Exception {
        this.database = database;
        fixture = new EngineFixture(database);
        fixture.createTopics(TOPIC.name(), UPSTREAM.name());
        fixture.start("consumer", List.of(Topic.of(TOPIC, batch -> batch.decide(task -> {
            done.countDown();
            return Decision.success();
        })).with(TopicSettings.defaults().withPollInterval(Duration.ofHours(1)))));
        Thread.sleep(200); // the consumer's first, empty poll has happened
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    @Test
    void a_push_in_a_bound_transaction_wakes_another_node_after_the_commit() throws Exception {
        JdbcTransactions transactions = new JdbcTransactions(database.dataSource());
        TaskEngine producer = fixture.create("producer", List.of(), settings -> settings.withTransactions(transactions));

        try (Connection listener = listen()) {
            transactions.inTransaction(connection -> {
                producer.tasks().push(TOPIC, "x", "payload");
                return null;
            });
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(heard(listener)).as("one notification, sent after the commit by the engine").hasSize(1);
        }
    }

    @Test
    void a_rolled_back_push_sends_nothing() throws Exception {
        JdbcTransactions transactions = new JdbcTransactions(database.dataSource());
        TaskEngine producer = fixture.create("producer", List.of(), settings -> settings.withTransactions(transactions));

        try (Connection listener = listen()) {
            try {
                transactions.inTransaction(connection -> {
                    producer.tasks().push(TOPIC, "x", "payload");
                    throw new IllegalStateException("rolled back");
                });
            } catch (IllegalStateException expected) {
                // the push is gone with the transaction
            }
            assertThat(heard(listener)).isEmpty();
        }
    }

    @Test
    void an_independent_push_wakes_another_node() throws Exception {
        TaskEngine producer = fixture.create("producer", List.of(), settings -> settings);

        producer.tasks().independently().push(TOPIC, "x", "payload");

        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void a_push_on_an_explicit_connection_still_notifies_at_its_commit() throws Exception {
        TaskEngine producer = fixture.create("producer", List.of(), settings -> settings);

        try (Connection connection = database.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            producer.tasks().on(connection).push(TOPIC, "x", "payload");
            connection.commit();
        }

        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void downstream_work_of_a_completion_wakes_another_node() throws Exception {
        TaskEngine upstream = fixture.start("upstream", List.of(Topic.of(UPSTREAM, batch -> batch.decide(task -> {
            task.push(TOPIC, task.identifier(), "from upstream");
            return Decision.success();
        }))));

        upstream.tasks().independently().push(UPSTREAM, "x", "payload");

        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void the_engine_debounces_its_notifications() throws Exception {
        TaskEngine producer = fixture.create("producer", List.of(), settings -> settings);

        try (Connection listener = listen()) {
            Thread[] threads = new Thread[8];
            for (int thread = 0; thread < threads.length; thread++) {
                int from = thread * 25;
                threads[thread] = Thread.ofVirtual().start(() -> {
                    for (int index = from; index < from + 25; index++) {
                        producer.tasks().independently().push(TOPIC, "x" + index, "payload");
                    }
                });
            }
            for (Thread thread : threads) {
                thread.join();
            }
            List<String> heard = heard(listener);
            assertThat(heard).isNotEmpty().hasSizeLessThan(200);
        }
    }

    private Connection listen() throws Exception {
        Connection connection = database.connect();
        try (Statement statement = connection.createStatement()) {
            statement.execute("LISTEN pgtasks_ready");
        }
        return connection;
    }

    /** Everything heard until half a second passes quietly. */
    private static List<String> heard(Connection listener) throws Exception {
        List<String> heard = new java.util.ArrayList<>();
        PGConnection connection = listener.unwrap(PGConnection.class);
        while (true) {
            PGNotification[] notifications = connection.getNotifications(500);
            if (notifications == null || notifications.length == 0) {
                return heard;
            }
            for (PGNotification notification : notifications) {
                heard.add(notification.getParameter());
            }
        }
    }
}
