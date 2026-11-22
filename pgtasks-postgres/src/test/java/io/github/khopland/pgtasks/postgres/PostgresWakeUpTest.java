package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.spi.TaskStore.WakeUp;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import static io.github.khopland.pgtasks.test.tck.StoreFixture.delayed;
import static io.github.khopland.pgtasks.test.tck.StoreFixture.task;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Who sends the wake-up for a push: the queue's trigger inside the transaction, or the caller
 * after the commit ({@link WakeUp#BY_CALLER}), so busy applications' commits don't queue for the
 * notify lock. Runs on the default schema and a configured one, whose flag carries its name.
 */
class PostgresWakeUpTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.empty();

    private PostgresDatabase database;
    private Connection listener;

    @BeforeEach
    void database(PostgresDatabase database) {
        this.database = database;
    }

    @AfterEach
    void close() throws SQLException {
        if (listener != null) {
            listener.close();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {TaskSchema.DEFAULT_SCHEMA, "orders_tasks"})
    void a_push_left_to_the_caller_does_not_notify_and_notify_ready_does(String schema) throws Exception {
        PostgresStoreFixture fixture = fixture(schema);
        int topic = fixture.topic("quiet");
        listen(schema);

        fixture.inTransaction(transaction -> fixture.store().push(transaction, topic, Insertion.APPEND, List.of(task("a")), WakeUp.BY_CALLER));
        assertThat(heard(300)).as("the commit sent nothing").isEmpty();

        fixture.inTransaction(transaction -> {
            fixture.store().notifyReady(transaction, List.of(topic));
            return null;
        });
        assertThat(heard(5_000)).containsExactly(String.valueOf(topic));
    }

    @ParameterizedTest
    @ValueSource(strings = {TaskSchema.DEFAULT_SCHEMA, "orders_tasks"})
    void a_push_at_commit_still_notifies(String schema) throws Exception {
        PostgresStoreFixture fixture = fixture(schema);
        int topic = fixture.topic("loud");
        listen(schema);

        fixture.inTransaction(transaction -> fixture.store().push(transaction, topic, Insertion.APPEND, List.of(task("a"))));

        assertThat(heard(5_000)).containsExactly(String.valueOf(topic));
    }

    @ParameterizedTest
    @ValueSource(strings = {TaskSchema.DEFAULT_SCHEMA, "orders_tasks"})
    void the_flag_covers_only_its_own_statement(String schema) throws Exception {
        PostgresStoreFixture fixture = fixture(schema);
        int quiet = fixture.topic("quiet");
        int loud = fixture.topic("loud");
        int raw = fixture.topic("raw");
        listen(schema);

        fixture.inTransaction(transaction -> {
            fixture.store().push(transaction, quiet, Insertion.APPEND, List.of(task("a")), WakeUp.BY_CALLER);
            fixture.store().push(transaction, loud, Insertion.APPEND, List.of(task("b")));
            fixture.store().push(transaction, quiet, Insertion.SUPERSEDE, List.of(task("c")), WakeUp.BY_CALLER);
            // An application's own insert later in the same transaction.
            try (Statement statement = transaction.connection().createStatement()) {
                statement.execute(TaskSchema.qualify("""
                    INSERT INTO pgtasks.task (topic_id, id, identifier) VALUES (%d, nextval('pgtasks.task_id_seq'), 'd');
                    INSERT INTO pgtasks.queue (task_id, topic_id, identifier, state) VALUES (currval('pgtasks.task_id_seq'), %d, 'd', 'ready');
                    """.formatted(raw, raw), schema));
            }
            return null;
        });

        assertThat(heard(5_000)).containsExactlyInAnyOrder(String.valueOf(loud), String.valueOf(raw));
        assertThat(database.count(TaskSchema.qualify("SELECT count(*) FROM pgtasks.queue", schema))).isEqualTo(4);
    }

    @ParameterizedTest
    @ValueSource(strings = {TaskSchema.DEFAULT_SCHEMA})
    void promotions_still_notify_after_a_quiet_push(String schema) throws Exception {
        PostgresStoreFixture fixture = fixture(schema);
        int topic = fixture.topic("later");
        fixture.inTransaction(transaction -> fixture.store().push(transaction, topic, Insertion.APPEND,
            List.of(delayed("a", java.time.Duration.ofSeconds(1))), WakeUp.BY_CALLER));
        fixture.advance(java.time.Duration.ofSeconds(2));
        listen(schema);

        fixture.inTransaction(transaction -> {
            fixture.store().push(transaction, topic, Insertion.APPEND, List.of(task("b")), WakeUp.BY_CALLER);
            return fixture.store().promoteDue(transaction, 100);
        });

        assertThat(heard(5_000)).as("the update trigger ignores the flag").containsExactly(String.valueOf(topic));
    }

    private PostgresStoreFixture fixture(String schema) throws SQLException {
        TaskSchema.migrate(database.dataSource(), schema);
        PostgresStoreFixture fixture = new PostgresStoreFixture(database, database.dataSource(), schema);
        fixture.store().prepare();
        return fixture;
    }

    private void listen(String schema) throws SQLException {
        listener = database.connect();
        try (Statement statement = listener.createStatement()) {
            statement.execute("LISTEN " + schema + "_ready");
        }
    }

    /** The ready notifications heard within the time, or until the first batch arrives and a short quiet follows. */
    private List<String> heard(int millis) throws SQLException {
        List<String> heard = new ArrayList<>();
        PGConnection connection = listener.unwrap(PGConnection.class);
        long deadline = System.nanoTime() + millis * 1_000_000L;
        while (System.nanoTime() < deadline) {
            PGNotification[] notifications = connection.getNotifications(heard.isEmpty() ? 100 : 200);
            if (notifications == null || notifications.length == 0) {
                if (!heard.isEmpty()) {
                    break;
                }
                continue;
            }
            for (PGNotification notification : notifications) {
                heard.add(notification.getParameter());
            }
        }
        return heard;
    }
}
