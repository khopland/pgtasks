package io.github.khopland.pgtasks.junit;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import io.github.khopland.pgtasks.BatchResource;
import io.github.khopland.pgtasks.Decisions;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.TestPostgres;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link TaskEngineExtension#withDataSource}: the tasks stay in memory, JDBC effects go to the database. */
class TaskEngineExtensionDataSourceTest {

    static final TopicRef<String> LEDGER = TopicRef.of("ledger", String.class);
    static final TopicRef<String> MAIL = TopicRef.of("mail", String.class);

    static final PostgresDatabase database = TestPostgres.createDatabase();
    /** The connections the engine took from the data source. */
    static final AtomicInteger connections = new AtomicInteger();
    static final List<String> mailed = new CopyOnWriteArrayList<>();

    static {
        database.execute("CREATE TABLE ledger (entry text NOT NULL)");
    }

    /** Writes the batch's tasks to the ledger with the completion. */
    static final BatchResource<Object> LEDGER_WRITER = new BatchResource<>() {
        @Override
        public Object open(TopicRef<?> topic, List<Task<?>> tasks) {
            return new Object();
        }

        @Override
        public void beforeCommit(Object resource, Decisions decisions, Connection connection) throws SQLException {
            try (PreparedStatement insert = connection.prepareStatement("INSERT INTO ledger VALUES (?)")) {
                for (Task<?> task : decisions.tasks()) {
                    insert.setString(1, String.valueOf(task.payload()));
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    };

    /** No JDBC effects: it sends after the commit. */
    static final BatchResource<Object> MAILER = new BatchResource<>() {
        @Override
        public Object open(TopicRef<?> topic, List<Task<?>> tasks) {
            return new Object();
        }

        @Override
        public void afterCommit(Object resource, Decisions decisions) {
            decisions.tasks().forEach(task -> mailed.add(String.valueOf(task.payload())));
        }
    };

    @RegisterExtension
    static TaskEngineExtension engine = TaskEngineExtension.of(tasks -> tasks
            .eachTask(LEDGER, task -> { }, options -> options.resources(LEDGER_WRITER))
            .eachTask(MAIL, task -> { }, options -> options.resources(MAILER)))
        .withDataSource(counting(database.dataSource()));

    @AfterAll
    static void close() {
        database.close();
    }

    @Test
    @DisplayName("A batch resource's beforeCommit writes land in the database")
    void jdbc_effects_are_written_to_the_data_source(Tasks tasks, TaskTester tester) {
        tasks.independently().push(LEDGER, "a", "first");
        tasks.independently().push(LEDGER, "b", "second");

        tester.drain(LEDGER);

        assertThat(database.rows("SELECT entry FROM ledger ORDER BY entry")).containsExactly(List.of("first"), List.of("second"));
    }

    @Test
    @DisplayName("A batch whose resources don't use the connection takes none")
    void a_resource_without_jdbc_effects_takes_no_connection(Tasks tasks, TaskTester tester) {
        connections.set(0);
        tasks.independently().push(MAIL, "a", "hello");

        tester.drain(MAIL);

        assertThat(mailed).containsExactly("hello");
        assertThat(connections).hasValue(0);
    }

    private static DataSource counting(DataSource target) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[] {DataSource.class},
            (proxy, method, arguments) -> {
                if (method.getName().equals("getConnection")) {
                    connections.incrementAndGet();
                }
                try {
                    return method.invoke(target, arguments);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
    }
}
