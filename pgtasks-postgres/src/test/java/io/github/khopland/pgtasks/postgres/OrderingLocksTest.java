package io.github.khopland.pgtasks.postgres;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The advisory lock that keeps SINGULAR_* claims apart (the race itself is in the TCK:
 * PollOrderingTck). FIFO claims, the hot path, take none.
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
