package com.example.orders;

import java.util.List;

import com.example.orders.OrderService.Line;
import com.example.orders.OrderService.PlaceOrder;
import com.example.orders.OrderService.Placed;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What only Postgres can prove: the order and its tasks commit and roll back together, and the
 * runtime role works without DDL rights against a schema Flyway migrated as the owner. A push-only
 * node (pgtasks.enabled=false), so nothing gets processed while the test looks.
 */
@Testcontainers
@SpringBootTest(properties = {"pgtasks.enabled=false", "server.port=0"})
class OrderServicePostgresTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17")
        .withDatabaseName("orders")
        .withCopyFileToContainer(MountableFile.forHostPath("docker/init.sql"), "/docker-entrypoint-initdb.d/init.sql");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", () -> "orders_app");
        registry.add("spring.datasource.password", () -> "app");
    }

    @Autowired
    OrderService orders;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    TransactionTemplate transactions;

    private int tasksFor(String orderId) {
        Integer count = jdbc.queryForObject("SELECT count(*) FROM pgtasks.task_view WHERE identifier = ?", Integer.class, orderId);
        return count == null ? 0 : count;
    }

    @Test
    void an_order_is_saved_with_its_payment_stock_and_ship_tasks() {
        Placed placed = orders.place(new PlaceOrder("req-1", "c-1", List.of(new Line("sku-1", 2))));

        assertThat(placed.created()).isTrue();
        assertThat(jdbc.queryForList("SELECT topic || ':' || state FROM pgtasks.task_view WHERE identifier = ? ORDER BY topic", String.class,
            placed.orderId())).containsExactly("orders.payment:ready", "orders.ship:suspended", "orders.stock:ready");
    }

    @Test
    void a_repeated_request_pushes_nothing_more() {
        Placed first = orders.place(new PlaceOrder("req-2", "c-1", List.of(new Line("sku-1", 1))));
        Placed again = orders.place(new PlaceOrder("req-2", "c-1", List.of(new Line("sku-1", 1))));

        assertThat(again).isEqualTo(new Placed(first.orderId(), false));
        assertThat(tasksFor(first.orderId())).isEqualTo(3);
    }

    @Test
    void a_rolled_back_order_leaves_no_tasks() {
        String[] orderId = new String[1];
        assertThatThrownBy(() -> transactions.executeWithoutResult(status -> {
            orderId[0] = orders.place(new PlaceOrder("req-3", "c-1", List.of(new Line("sku-1", 1)))).orderId();
            throw new IllegalStateException("the caller's transaction fails after the push");
        })).hasMessageContaining("after the push");

        assertThat(jdbc.queryForObject("SELECT count(*) FROM orders WHERE id = ?", Integer.class, orderId[0])).isZero();
        assertThat(tasksFor(orderId[0])).isZero();
    }
}
