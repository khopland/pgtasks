package io.github.khopland.pgtasks.postgres;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.SchemaMode;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Review 2026-11-25: a node that went silent inside one of the store's transactions (a frozen VM,
 * a network that drops packets without closing the connection) kept that transaction, and its
 * locks, until the server's TCP keepalive gave up on it: hours by default. A SINGULAR_* claim
 * held its topic's lock, so no node could claim from the topic; a lease renewal held the lease
 * row, so every other node's heartbeat waited. On the node itself, a statement whose reply was
 * lost waited forever, and with it the topic's poller.
 */
class StuckTransactionTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresStoreFixture.extension();

    private static final Duration STUCK = Duration.ofSeconds(1);

    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void close() {
        for (AutoCloseable resource : resources.reversed()) {
            try {
                resource.close();
            } catch (Exception ignored) {
                // A stuck transaction's session is gone.
            }
        }
    }

    private PostgresTaskStore store(DataSource dataSource, Duration networkTimeout) {
        return new PostgresTaskStore(dataSource, SchemaMode.NONE, TaskSchema.DEFAULT_SCHEMA, STUCK, networkTimeout);
    }

    private static <T> T within(Duration limit, Callable<T> work) throws Exception {
        CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
            try {
                return work.call();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }, task -> Thread.ofVirtual().start(task));
        try {
            return future.get(limit.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw e.getCause() instanceof IllegalStateException wrapped && wrapped.getCause() instanceof Exception cause ? cause : e;
        }
    }

    private static <T> T transact(TaskStore store, StoreFixture.Work<T> work) throws Exception {
        try (StoreTransaction transaction = store.begin()) {
            T result = work.run(transaction);
            transaction.commit();
            return result;
        }
    }

    @Test
    void a_claim_stuck_before_its_commit_holds_its_singular_topic_only_until_the_timeout(PostgresDatabase database) throws Exception {
        PostgresTaskStore store = store(database.dataSource(), PostgresTaskStore.NETWORK_TIMEOUT);
        int topic = transact(store, transaction -> store.createTopic(transaction, "singular"));
        int frozen = transact(store, transaction -> store.registerNode(transaction, "frozen", null));
        int healthy = transact(store, transaction -> store.registerNode(transaction, "healthy", null));
        transact(store, transaction -> store.push(transaction, topic, Insertion.APPEND, List.of(StoreFixture.task("a"), StoreFixture.task("b"))));
        StoreTransaction stuck = store.begin();
        resources.add(stuck);
        assertThat(store.claim(stuck, new TaskStore.ClaimRequest(topic, frozen, Ordering.SINGULAR_BY_IDENTIFIER, 1)).tasks())
            .as("holds the topic's claim lock, and never commits").hasSize(1);

        long started = System.nanoTime();
        List<TaskStore.ClaimedTask> claimed = within(Duration.ofSeconds(20), () -> transact(store,
            transaction -> store.claim(transaction, new TaskStore.ClaimRequest(topic, healthy, Ordering.SINGULAR_BY_IDENTIFIER, 10)).tasks()));

        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(10));
        assertThat(claimed).as("the stuck claim went with its session, so both tasks are ready again").hasSize(2);
    }

    @Test
    void a_lease_renewal_stuck_before_its_commit_holds_up_other_nodes_only_until_the_timeout(PostgresDatabase database) throws Exception {
        PostgresTaskStore store = store(database.dataSource(), PostgresTaskStore.NETWORK_TIMEOUT);
        int frozen = transact(store, transaction -> store.registerNode(transaction, "frozen", null));
        int healthy = transact(store, transaction -> store.registerNode(transaction, "healthy", null));
        StoreTransaction stuck = store.begin();
        resources.add(stuck);
        assertThat(store.lead(stuck, "housekeeping", frozen, Duration.ofMillis(1))).as("takes the row, and never commits").isTrue();

        boolean led = within(Duration.ofSeconds(20), () -> transact(store, transaction -> store.lead(transaction, "housekeeping", healthy, Duration.ofMinutes(1))));

        assertThat(led).as("the stuck renewal went with its session").isTrue();
    }

    @Test
    void a_reply_lost_on_a_silent_connection_fails_the_statement_instead_of_waiting_forever(PostgresDatabase database) throws Exception {
        TcpProxy proxy = TcpProxy.to(database);
        resources.add(proxy);
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(proxy.jdbcUrl(database));
        config.setUsername(database.user());
        config.setPassword(database.password());
        config.setMaximumPoolSize(2);
        HikariDataSource pool = new HikariDataSource(config);
        resources.add(pool);
        PostgresTaskStore store = store(pool, Duration.ofSeconds(1));
        int topic = transact(store, transaction -> store.createTopic(transaction, "work"));
        int node = transact(store, transaction -> store.registerNode(transaction, "node", null));
        StoreTransaction transaction = store.begin();
        resources.add(transaction);
        store.topics(transaction);
        proxy.blackholeOpenLinks();

        long started = System.nanoTime();
        Exception failure = within(Duration.ofSeconds(20), () -> {
            try {
                store.claim(transaction, new TaskStore.ClaimRequest(topic, node, Ordering.FIFO, 10));
                return null;
            } catch (SQLException e) {
                return e;
            }
        });

        assertThat(failure).as("the claim gives the connection up").isInstanceOf(SQLException.class);
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(10));
    }
}
