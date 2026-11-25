package io.github.khopland.pgtasks.engine;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.DeadLetterQuery;
import io.github.khopland.pgtasks.admin.TaskQuery;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.spi.TaskStore;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Review 2026-11-23: a bulk operator action paged with "after the batch's highest id", and the
 * store skips rows locked at that moment, so a row locked by a claim, a promotion or a push while
 * its batch ran was never taken by that call.
 */
class BulkActionLocksTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    private PostgresDatabase database;
    /** Holds a row lock until the action, done with its batches, reads which rows are left. */
    private final AtomicReference<Connection> holder = new AtomicReference<>();
    private TaskEngine engine;

    @BeforeEach
    void setUp(PostgresDatabase database) throws Exception {
        this.database = database;
        PostgresTaskStore postgresStore = new PostgresTaskStore(database.dataSource());
        TaskStore store = (TaskStore) Proxy.newProxyInstance(TaskStore.class.getClassLoader(), new Class<?>[] {TaskStore.class},
            (proxy, called, args) -> {
                if (called.getName().equals("deadLetters") || called.getName().equals("tasks")) {
                    release();
                }
                try {
                    return called.invoke(postgresStore, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
        new EngineFixture(database).createTopics(WORK.name());
        engine = new TaskEngine(store, EngineFixture.settings("operator"), List.of()).prepare();
    }

    @AfterEach
    void tearDown() throws Exception {
        release();
        engine.close();
    }

    private void lock(long taskId) throws Exception {
        Connection connection = database.dataSource().getConnection();
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("SELECT task_id FROM pgtasks.queue WHERE task_id = ? FOR UPDATE")) {
            statement.setLong(1, taskId);
            try (ResultSet resultSet = statement.executeQuery()) {
                assertThat(resultSet.next()).isTrue();
            }
        }
        holder.set(connection);
    }

    private void release() throws Exception {
        Connection connection = holder.getAndSet(null);
        if (connection != null) {
            connection.rollback();
            connection.close();
        }
    }

    private List<Long> push(int count) {
        return IntStream.range(0, count)
            .mapToObj(index -> engine.tasks().independently().push(WORK, "task-" + index, "payload").id())
            .toList();
    }

    @Test
    void a_revive_takes_a_dead_letter_that_was_locked_while_its_batch_ran() throws Exception {
        List<Long> ids = push(3);
        database.execute("UPDATE pgtasks.queue SET state = 'failed', failures = 1, first_failed_at = now()");
        lock(ids.getFirst());

        assertThat(engine.admin().revive(WORK.name(), DeadLetterQuery.ALL)).isEqualTo(3);
        assertThat(database.count("SELECT count(*) FROM pgtasks.queue WHERE state = 'failed'")).isZero();
    }

    @Test
    void a_release_takes_a_suspended_task_that_was_locked_while_its_batch_ran() throws Exception {
        List<Long> ids = push(3);
        database.execute("UPDATE pgtasks.queue SET state = 'suspended'");
        lock(ids.getFirst());

        assertThat(engine.admin().release(WORK.name(), TaskQuery.ALL)).isEqualTo(3);
        assertThat(database.count("SELECT count(*) FROM pgtasks.queue WHERE state = 'ready'")).isEqualTo(3);
    }

    @Test
    void a_task_still_locked_is_left_and_not_counted() throws Exception {
        List<Long> ids = push(3);
        database.execute("UPDATE pgtasks.queue SET state = 'failed', failures = 1, first_failed_at = now()");
        lock(ids.get(1));
        // Keep the lock through every rescan.
        Connection kept = holder.getAndSet(null);
        try {
            assertThat(engine.admin().discard(WORK.name(), DeadLetterQuery.ALL)).isEqualTo(2);
        } finally {
            kept.rollback();
            kept.close();
        }
        assertThat(database.count("SELECT count(*) FROM pgtasks.queue")).isEqualTo(1);
    }
}
