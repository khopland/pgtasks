package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;
import io.github.khopland.pgtasks.test.tck.JunctionTck;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The fan-in TCK on a database whose default isolation is REPEATABLE READ, as a pool's
 * transactionIsolation or a role's default_transaction_isolation can make it. The store pins its
 * own transactions to READ COMMITTED; without that, two parts completing at once each miss the
 * other and the group stays suspended.
 */
class RepeatableReadDefaultTest extends JunctionTck {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> {
        // Before the first pool connects, so every connection the tests get starts at the default.
        database.execute("ALTER DATABASE " + database.name() + " SET default_transaction_isolation = 'repeatable read'");
        TaskSchema.migrate(database.dataSource());
    });

    private PostgresDatabase database;

    @BeforeEach
    void database(PostgresDatabase database) {
        this.database = database;
    }

    @Override
    protected StoreFixture createFixture() {
        return new PostgresStoreFixture(database);
    }

    @Test
    void the_stores_transactions_run_at_read_committed_and_the_connection_keeps_its_default() throws SQLException {
        // One connection, so the second transaction and the check after get the same one back.
        HikariDataSource pool = database.newPool("single", 1);
        PostgresTaskStore store = new PostgresTaskStore(pool, SchemaMode.NONE, TaskSchema.DEFAULT_SCHEMA);
        for (int round = 0; round < 2; round++) {
            try (StoreTransaction transaction = store.begin()) {
                assertThat(isolation(transaction.connection())).isEqualTo("read committed");
                transaction.commit();
            }
        }
        try (Connection connection = pool.getConnection()) {
            assertThat(isolation(connection)).isEqualTo("repeatable read");
        }
    }

    private static String isolation(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW transaction_isolation")) {
            resultSet.next();
            return resultSet.getString(1);
        }
    }
}
