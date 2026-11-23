package io.github.khopland.pgtasks.postgres;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.github.khopland.pgtasks.spi.Notifier;
import io.github.khopland.pgtasks.spi.TaskStore;
import io.github.khopland.pgtasks.spi.TaskStore.ClaimedTask;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.ds.PGSimpleDataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A configured schema name: checked, used everywhere, and kept apart from another schema in the same database. */
class SchemaNameTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.empty();

    @ParameterizedTest
    @ValueSource(strings = {"tasks", "pgtasks", "billing_tasks", "_q", "a1", "x23456789_123456789_123456789_123456789_1234567890"})
    void accepts_plain_lowercase_identifiers(String schema) {
        assertThat(TaskSchema.validName(schema)).isEqualTo(schema);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "Tasks", "1tasks", "my-tasks", "my tasks", "tasks.x", "\"tasks\"", "tasks;drop", "pg_tasks",
        "x23456789_123456789_123456789_123456789_1234567890x"})
    void refuses_anything_else(String schema) {
        assertThatThrownBy(() -> TaskSchema.validName(schema)).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("plain lowercase identifier");
        assertThatThrownBy(() -> new PostgresTaskStore(new PGSimpleDataSource(), SchemaMode.NONE, schema))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refuses_sql_keywords_when_it_migrates(PostgresDatabase database) {
        for (String keyword : List.of("user", "select", "table", "int")) {
            assertThatThrownBy(() -> TaskSchema.migrate(database.dataSource(), keyword))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SQL keyword");
        }
        assertThat(database.count("SELECT count(*) FROM pg_namespace WHERE nspname IN ('user', 'select', 'table', 'int')")).isZero();
    }

    @Test
    void the_provider_and_its_options_pick_the_schema(PostgresDatabase database) throws SQLException {
        TaskStore store = new PostgresStoreProvider().create(database.dataSource(), Map.of("schema", "billing_tasks"));
        store.prepare();

        assertThat(database.count("SELECT count(*) FROM pg_namespace WHERE nspname = 'billing_tasks'")).isOne();
        assertThat(database.count("SELECT count(*) FROM pg_namespace WHERE nspname = 'pgtasks'")).isZero();
        assertThat(database.count("SELECT count(*) FROM billing_tasks.schema_history")).isEqualTo(TaskSchema.FILES.size());
    }

    @Test
    void two_schemas_share_a_database_without_seeing_each_other(PostgresDatabase database) throws Exception {
        TaskSchema.migrate(database.dataSource(), "orders_tasks");
        TaskSchema.migrate(database.dataSource(), "billing_tasks");
        PostgresStoreFixture orders = new PostgresStoreFixture(database, database.dataSource(), "orders_tasks");
        PostgresStoreFixture billing = new PostgresStoreFixture(database, database.dataSource(), "billing_tasks");
        orders.store().prepare();
        billing.store().prepare();

        int ordersTopic = orders.topic("invoice");
        int billingTopic = billing.topic("invoice");
        orders.push(ordersTopic, "order-1");
        orders.push(ordersTopic, "order-2");
        billing.push(billingTopic, "bill-1");

        assertThat(database.count("SELECT count(*) FROM orders_tasks.queue")).isEqualTo(2);
        assertThat(database.count("SELECT count(*) FROM billing_tasks.queue")).isOne();
        assertThat(orders.claim(ordersTopic, orders.node("a"))).extracting(ClaimedTask::identifier).containsExactly("order-1", "order-2");
        assertThat(billing.claim(billingTopic, billing.node("b"))).extracting(ClaimedTask::identifier).containsExactly("bill-1");
        assertThat(TaskSchema.migrate(database.dataSource(), "orders_tasks").applied()).as("each schema keeps its own history").isEmpty();
    }

    @Test
    void notifications_stay_within_their_schema(PostgresDatabase database) throws Exception {
        TaskSchema.migrate(database.dataSource(), "orders_tasks");
        TaskSchema.migrate(database.dataSource());
        PostgresStoreFixture orders = new PostgresStoreFixture(database, database.dataSource(), "orders_tasks");
        PostgresStoreFixture tasks = new PostgresStoreFixture(database);
        Recorder ordersHeard = new Recorder(), tasksHeard = new Recorder();
        try (Notifier ordersNotifier = new PostgresNotifier(database::connect, "orders_tasks");
             Notifier tasksNotifier = new PostgresNotifier(database::connect)) {
            ordersNotifier.start(ordersHeard, Set.of(Notifier.TOPIC));
            tasksNotifier.start(tasksHeard, Set.of(Notifier.TOPIC));
            awaitConnected(ordersNotifier);
            awaitConnected(tasksNotifier);

            int ordersTopic = orders.topic("orders-only");
            orders.push(ordersTopic, "order-1");
            assertThat(ordersHeard.events.poll(5, TimeUnit.SECONDS)).isEqualTo("topic orders-only");
            assertThat(ordersHeard.events.poll(5, TimeUnit.SECONDS)).isEqualTo("ready " + ordersTopic);

            int tasksTopic = tasks.topic("tasks-only");
            tasks.push(tasksTopic, "task-1");
            assertThat(tasksHeard.events.poll(5, TimeUnit.SECONDS)).isEqualTo("topic tasks-only");
            assertThat(tasksHeard.events.poll(5, TimeUnit.SECONDS)).isEqualTo("ready " + tasksTopic);

            assertThat(ordersHeard.events.poll(300, TimeUnit.MILLISECONDS)).as("orders heard nothing of tasks").isNull();
            assertThat(tasksHeard.events).as("tasks heard nothing of orders").isEmpty();
        }
    }

    private static void awaitConnected(Notifier notifier) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!notifier.connected() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(notifier.connected()).isTrue();
    }

    private static final class Recorder implements Notifier.Listener {

        final BlockingQueue<String> events = new LinkedBlockingQueue<>();

        @Override
        public void ready(int topicId) {
            events.add("ready " + topicId);
        }

        @Override
        public void notified(String channel, String payload) {
            events.add(channel + " " + payload);
        }

        @Override
        public void missed() {
            events.add("missed");
        }
    }
}
