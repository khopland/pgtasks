package io.github.khopland.pgtasks.postgres;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Which clock each time comes from: the database's, and for a push, the push's own moment. */
class PostgresTimeTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresStoreFixture.extension();

    private PostgresStoreFixture fixture;
    private int topic;

    @BeforeEach
    void setUp(PostgresDatabase database) throws SQLException {
        fixture = new PostgresStoreFixture(database);
        topic = fixture.topic("topic");
    }

    @Test
    void the_store_clock_is_the_database_clock() throws SQLException {
        fixture.node("node-a");
        try (StoreTransaction transaction = fixture.store().begin()) {
            Instant database = databaseNow(transaction);
            assertThat(fixture.store().clock().instant()).isCloseTo(database, within(Duration.ofMillis(500)));
        }
    }

    @Test
    void a_delay_counts_from_the_push_not_from_the_start_of_its_transaction() throws SQLException {
        try (StoreTransaction transaction = fixture.store().begin()) {
            sleep(transaction, Duration.ofMillis(1500));
            fixture.store().push(transaction, topic, Insertion.APPEND,
                List.of(new TaskStore.NewTask("a", null, null, null, Duration.ofSeconds(2), false, 0, null, null, null, Map.of())));

            try (PreparedStatement statement = transaction.connection().prepareStatement(
                "SELECT q.state::text, q.run_at, t.created_at, clock_timestamp() FROM pgtasks.queue q JOIN pgtasks.task t ON t.id = q.task_id");
                 ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                Instant pushedBy = resultSet.getObject(4, OffsetDateTime.class).toInstant();
                Instant runAt = resultSet.getObject(2, OffsetDateTime.class).toInstant();
                Instant createdAt = resultSet.getObject(3, OffsetDateTime.class).toInstant();
                assertThat(resultSet.getString(1)).isEqualTo("scheduled");
                assertThat(Duration.between(pushedBy, runAt)).as("the whole delay is still ahead").isGreaterThan(Duration.ofMillis(1800));
                assertThat(Duration.between(createdAt, pushedBy)).as("created when pushed, not when the transaction began").isLessThan(Duration.ofMillis(500));
            }
        }
    }

    @Test
    void a_run_time_reached_while_the_transaction_ran_is_ready_at_once() throws SQLException {
        try (StoreTransaction transaction = fixture.store().begin()) {
            Instant runAt = databaseNow(transaction).plusMillis(500);
            sleep(transaction, Duration.ofSeconds(1));
            fixture.store().push(transaction, topic, Insertion.APPEND,
                List.of(new TaskStore.NewTask("a", null, null, null, null, false, 0, null, null, null, Map.of(), runAt)));

            try (PreparedStatement statement = transaction.connection().prepareStatement("SELECT state::text FROM pgtasks.queue");
                 ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                assertThat(resultSet.getString(1)).isEqualTo("ready");
            }
        }
    }

    private static Instant databaseNow(StoreTransaction transaction) throws SQLException {
        try (PreparedStatement statement = transaction.connection().prepareStatement("SELECT clock_timestamp()");
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getObject(1, OffsetDateTime.class).toInstant();
        }
    }

    private static void sleep(StoreTransaction transaction, Duration duration) throws SQLException {
        try (PreparedStatement statement = transaction.connection().prepareStatement("SELECT pg_sleep(" + duration.toMillis() / 1000.0 + ")")) {
            statement.execute();
        }
    }
}
