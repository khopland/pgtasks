package io.github.khopland.pgtasks.postgres;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore.NewTask;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Review 2026-11-25: a SUPERSEDE or REPLACE push took one advisory lock per identifier, each a slot
 * in the server's shared lock table. A push of 100,000 identifiers failed with "out of shared
 * memory", and one of a few thousand on a server with default settings could make other sessions'
 * queries fail the same way at the same moment.
 */
class PushLocksTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresStoreFixture.extension();

    private final List<StoreTransaction> open = new ArrayList<>();

    @AfterEach
    void close() throws SQLException {
        for (StoreTransaction transaction : open) {
            transaction.close();
        }
    }

    private static NewTask[] tasks(String prefix, int count) {
        return IntStream.range(0, count).mapToObj(index -> StoreFixture.task(prefix + index)).toArray(NewTask[]::new);
    }

    private StoreTransaction begin(PostgresTaskStore store) throws SQLException {
        StoreTransaction transaction = store.begin();
        open.add(transaction);
        return transaction;
    }

    private static int pid(StoreTransaction transaction) throws SQLException {
        try (Statement statement = transaction.connection().createStatement(); ResultSet resultSet = statement.executeQuery("SELECT pg_backend_pid()")) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    private static long advisoryLocks(PostgresDatabase database, int pid, boolean granted) {
        return database.count("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = ? AND granted = ?", pid, granted);
    }

    @Test
    void a_push_of_many_identifiers_locks_its_topic_once_instead_of_each(PostgresDatabase database) throws Exception {
        PostgresTaskStore store = new PostgresTaskStore(database.dataSource());
        int topic = StoreFixtureSupport.topic(store, "work");
        StoreTransaction transaction = begin(store);

        List<Pushed> pushed = store.push(transaction, topic, Insertion.SUPERSEDE, List.of(tasks("many-", 100_000)));

        assertThat(pushed).hasSize(100_000).allMatch(each -> each.status() == Pushed.Status.QUEUED);
        assertThat(advisoryLocks(database, pid(transaction), true)).isEqualTo(1);
        transaction.commit();
    }

    @Test
    void a_push_of_a_few_identifiers_locks_each_of_them_and_shares_its_topic(PostgresDatabase database) throws Exception {
        PostgresTaskStore store = new PostgresTaskStore(database.dataSource());
        int topic = StoreFixtureSupport.topic(store, "work");
        StoreTransaction transaction = begin(store);

        store.push(transaction, topic, Insertion.REPLACE, List.of(tasks("few-", PostgresTaskStore.MAX_IDENTIFIER_LOCKS)));

        assertThat(advisoryLocks(database, pid(transaction), true)).isEqualTo(PostgresTaskStore.MAX_IDENTIFIER_LOCKS + 1);
    }

    @Test
    void a_large_push_and_the_topics_small_ones_wait_for_each_other_and_no_other_topic(PostgresDatabase database) throws Exception {
        PostgresTaskStore store = new PostgresTaskStore(database.dataSource());
        int topic = StoreFixtureSupport.topic(store, "work");
        int other = StoreFixtureSupport.topic(store, "other");
        StoreTransaction large = begin(store);
        store.push(large, topic, Insertion.SUPERSEDE, List.of(tasks("large-", PostgresTaskStore.MAX_IDENTIFIER_LOCKS + 1)));

        StoreTransaction elsewhere = begin(store);
        store.push(elsewhere, other, Insertion.SUPERSEDE, List.of(tasks("large-", 3)));
        elsewhere.commit();

        StoreTransaction small = begin(store);
        int waiting = pid(small);
        CompletableFuture<List<Pushed>> blocked = CompletableFuture.supplyAsync(() -> {
            try {
                return store.push(small, topic, Insertion.SUPERSEDE, List.of(tasks("large-", 2)));
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }, task -> Thread.ofVirtual().start(task));
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (advisoryLocks(database, waiting, false) == 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }

        assertThat(advisoryLocks(database, waiting, false)).as("waits for the topic").isEqualTo(1);
        assertThat(advisoryLocks(database, waiting, true)).as("holding no identifier's lock while it waits").isZero();
        large.commit();
        assertThat(blocked.get(10, TimeUnit.SECONDS)).extracting(Pushed::status).containsOnly(Pushed.Status.QUEUED);
        small.commit();
        assertThat(database.count("SELECT count(*) FROM pgtasks.queue WHERE topic_id = ?", topic))
            .as("the later push superseded the first two of the large one's").isEqualTo(PostgresTaskStore.MAX_IDENTIFIER_LOCKS + 1);
    }

    /** Topic creation in a transaction of its own. */
    private static final class StoreFixtureSupport {

        static int topic(PostgresTaskStore store, String name) throws SQLException {
            try (StoreTransaction transaction = store.begin()) {
                int id = store.createTopic(transaction, name);
                transaction.commit();
                return id;
            }
        }
    }
}
