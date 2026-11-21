package io.github.khopland.pgtasks.spring.modulith.shop.orders;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.spring.modulith.shop.orders.api.OrderTopics;
import io.github.khopland.pgtasks.spring.modulith.shop.orders.internal.OrderBook;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestConstructor;
import org.springframework.modulith.test.ApplicationModuleTest;
import org.springframework.test.context.TestPropertySource;

/** The orders module alone: nobody routes its event, so only its own topic gets a task. */
@ApplicationModuleTest
@TestPropertySource(properties = "pgtasks.store=memory")
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
class OrdersModuleTest {

    private final OrderService orders;
    private final TaskTester tester;
    private final TaskEngine engine;
    private final OrderBook book;

    OrdersModuleTest(OrderService orders, TaskTester tester, TaskEngine engine, OrderBook book) {
        this.orders = orders;
        this.tester = tester;
        this.engine = engine;
        this.book = book;
    }

    @Test
    void a_placed_order_is_booked() {
        orders.place("o-3");
        tester.awaitIdle();

        assertThat(engine.processing()).containsExactly("orders");
        assertThat(engine.eventRoutes()).isEmpty();
        assertThat(book.orders).containsExactly("o-3");
        assertThat(tester.captured()).extracting(captured -> captured.topic().name()).containsExactly(OrderTopics.ORDERS.name());
    }
}
