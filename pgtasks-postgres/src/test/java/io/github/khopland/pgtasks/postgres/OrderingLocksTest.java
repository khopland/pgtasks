package io.github.khopland.pgtasks.postgres;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static io.github.khopland.pgtasks.test.tck.StoreFixture.task;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The advisory locks that keep per-identifier order (the races themselves are in the TCK:
 * PollOrderingTck and PushTck). They are taken only where they are needed: APPEND pushes and
 * FIFO claims, the hot paths, take none.
 */
class OrderingLocksTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresStoreFixture.extension();

    private PostgresStoreFixture fixture;
    private int topic, node;

    @BeforeEach
    void setUp(PostgresDatabase database) throws SQLException {
        fixture = new PostgresStoreFixture(database);
        topic = fixture.topic("topic");
        node = fixture.node("node-a");
    }

    @Test
    void an_append_push_takes_no_lock() throws SQLException {
        try (StoreTransaction transaction = fixture.store().begin()) {
            fixture.store().push(transaction, topic, Insertion.APPEND, List.of(task("x"), task("y"), task("x")));

            assertThat(advisoryLocks(transaction)).isZero();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Insertion.class, names = "APPEND", mode = EnumSource.Mode.EXCLUDE)
    void other_pushes_lock_each_identifier_once(Insertion insertion) throws SQLException {
        try (StoreTransaction transaction = fixture.store().begin()) {
            fixture.store().push(transaction, topic, insertion, List.of(task("x"), task("y"), task("x")));

            assertThat(advisoryLocks(transaction)).isEqualTo(2);
        }
    }

    @Test
    void a_fifo_claim_takes_no_lock() throws SQLException {
        fixture.push(topic, "x");
        try (StoreTransaction transaction = fixture.store().begin()) {
            fixture.store().claim(transaction, new TaskStore.ClaimRequest(topic, node, Ordering.FIFO, 10, null));

            assertThat(advisoryLocks(transaction)).isZero();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Ordering.class, names = "SINGULAR.*", mode = EnumSource.Mode.MATCH_ALL)
    void a_singular_claim_locks_its_topic(Ordering ordering) throws SQLException {
        int other = fixture.topic("other");
        try (StoreTransaction transaction = fixture.store().begin()) {
            fixture.store().claim(transaction, new TaskStore.ClaimRequest(topic, node, ordering, 10, null));

            assertThat(advisoryLocks(transaction)).isOne();
            // Another topic's claims don't wait for this one.
            try (StoreTransaction elsewhere = fixture.store().begin()) {
                fixture.store().claim(elsewhere, new TaskStore.ClaimRequest(other, node, ordering, 10, null));
                assertThat(advisoryLocks(elsewhere)).isOne();
            }
        }
    }

    private static long advisoryLocks(StoreTransaction transaction) throws SQLException {
        try (PreparedStatement statement = transaction.connection().prepareStatement(
                 "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted");
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }
}
