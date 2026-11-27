package io.github.khopland.pgtasks.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.InstantSource;
import java.util.List;

import javax.sql.DataSource;

import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Finding 10 of the order-service run: batch resources' JDBC effects on the in-memory store. */
class MemoryJdbcEffectsTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> database.execute("""
        CREATE TABLE ledger (entry text NOT NULL,
                             CONSTRAINT one_entry UNIQUE (entry) DEFERRABLE INITIALLY DEFERRED)
        """));

    private static void insert(Connection connection, String entry) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO ledger VALUES ('" + entry + "')");
        }
    }

    @Test
    @DisplayName("Without a data source, using the connection fails and says what to do")
    void without_a_data_source_the_connection_fails_clearly() throws SQLException {
        InMemoryTaskStore store = new InMemoryTaskStore();
        try (StoreTransaction transaction = store.begin()) {
            assertThatThrownBy(() -> insert(transaction.connection(), "a"))
                .isInstanceOf(SQLFeatureNotSupportedException.class)
                .hasMessageContaining("given a DataSource");
        }
    }

    @Test
    @DisplayName("The data source is used only when the connection is: handing it out opens nothing")
    void the_connection_opens_on_first_use() throws SQLException {
        DataSource unreachable = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[] {DataSource.class},
            (proxy, method, arguments) -> {
                throw new SQLException("the database is down");
            });
        InMemoryTaskStore store = new InMemoryTaskStore(InstantSource.system(), unreachable);

        try (StoreTransaction transaction = store.begin()) {
            store.createTopic(transaction, "untouched");
            Connection handed = transaction.connection();
            assertThat(handed).isSameAs(transaction.connection());
            transaction.commit();
        }
        try (StoreTransaction transaction = store.begin()) {
            assertThat(store.findTopic(transaction, "untouched")).isPresent();
            assertThatThrownBy(() -> insert(transaction.connection(), "a")).isInstanceOf(SQLException.class).hasMessage("the database is down");
        }
    }

    @Test
    @DisplayName("JDBC effects commit with the store's changes, and roll back with them")
    void effects_commit_and_roll_back_with_the_store(PostgresDatabase database) throws SQLException {
        InMemoryTaskStore store = new InMemoryTaskStore(InstantSource.system(), database.dataSource());

        try (StoreTransaction transaction = store.begin()) {
            store.createTopic(transaction, "kept");
            insert(transaction.connection(), "kept");
            assertThat(database.count("SELECT count(*) FROM ledger")).as("not visible before the commit").isZero();
            transaction.commit();
        }
        try (StoreTransaction transaction = store.begin()) {
            store.createTopic(transaction, "dropped");
            insert(transaction.connection(), "dropped");
            transaction.rollback();
        }
        try (StoreTransaction transaction = store.begin()) {
            store.createTopic(transaction, "closed");
            insert(transaction.connection(), "closed");
        }

        assertThat(database.rows("SELECT entry FROM ledger")).containsExactly(List.of("kept"));
        try (StoreTransaction transaction = store.begin()) {
            assertThat(store.findTopic(transaction, "kept")).isPresent();
            assertThat(store.findTopic(transaction, "dropped")).isEmpty();
            assertThat(store.findTopic(transaction, "closed")).isEmpty();
        }
    }

    @Test
    @DisplayName("A savepoint undoes the JDBC effects made after it, opened before them or not")
    void a_savepoint_rolls_back_effects_after_it(PostgresDatabase database) throws SQLException {
        InMemoryTaskStore store = new InMemoryTaskStore(InstantSource.system(), database.dataSource());

        try (StoreTransaction transaction = store.begin()) {
            StoreTransaction.Savepoint first = transaction.savepoint();
            insert(transaction.connection(), "undone");
            store.createTopic(transaction, "undone");
            first.rollback();
            insert(transaction.connection(), "kept");
            StoreTransaction.Savepoint second = transaction.savepoint();
            insert(transaction.connection(), "released");
            second.release();
            transaction.commit();
        }

        assertThat(database.rows("SELECT entry FROM ledger ORDER BY entry"))
            .containsExactly(List.of("kept"), List.of("released"));
        try (StoreTransaction transaction = store.begin()) {
            assertThat(store.findTopic(transaction, "undone")).isEmpty();
        }
    }

    @Test
    @DisplayName("A JDBC commit that fails rolls back the store's changes too")
    void a_failed_jdbc_commit_rolls_back_the_store(PostgresDatabase database) throws SQLException {
        InMemoryTaskStore store = new InMemoryTaskStore(InstantSource.system(), database.dataSource());

        try (StoreTransaction transaction = store.begin()) {
            store.createTopic(transaction, "work");
            insert(transaction.connection(), "twice");
            insert(transaction.connection(), "twice");   // the deferred constraint fails the commit
            assertThatThrownBy(transaction::commit).isInstanceOf(SQLException.class);
        }

        assertThat(database.count("SELECT count(*) FROM ledger")).isZero();
        try (StoreTransaction transaction = store.begin()) {
            assertThat(store.findTopic(transaction, "work")).as("rolled back, and the store's lock released").isEmpty();
        }
    }
}
