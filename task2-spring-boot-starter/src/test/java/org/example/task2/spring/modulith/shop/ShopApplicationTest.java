package org.example.task2.spring.modulith.shop;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.example.task2.engine.EffectiveSettings;
import org.example.task2.engine.EngineFixture;
import org.example.task2.engine.TaskEngine;
import org.example.task2.engine.TaskTester;
import org.example.task2.spring.modulith.shop.orders.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestConstructor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.modulith.test.TestApplicationModules;

/** The whole application: every module's topics run, and an order goes through all three. */
@SpringBootTest(classes = ShopApplication.class, properties = {"task.store=memory", "task.testing=true"})
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
class ShopApplicationTest {

    private final OrderService orders;
    private final TaskEngine engine;
    private final TaskTester tester;

    ShopApplicationTest(OrderService orders, TaskEngine engine, TaskTester tester) {
        this.orders = orders;
        this.engine = engine;
        this.tester = tester;
    }

    @Test
    void the_modules_depend_only_on_each_others_topics_and_events() {
        TestApplicationModules.of(ShopApplication.class).verify();
    }

    @Test
    void topics_are_shown_by_the_module_that_registered_them() {
        assertThat(engine.effectiveSettings()).extracting(EffectiveSettings::topic, EffectiveSettings::module)
            .containsExactlyInAnyOrder(
                org.assertj.core.groups.Tuple.tuple("orders", "orders"),
                org.assertj.core.groups.Tuple.tuple("shipments", "shipping"),
                org.assertj.core.groups.Tuple.tuple("invoices", "billing"));
    }

    @Test
    void an_order_is_booked_shipped_through_the_event_and_invoiced() throws Exception {
        orders.place("o-1");
        EngineFixture.await("the invoice", Duration.ofSeconds(5), () -> tester.captured().size() == 3);
        tester.awaitIdle();

        // Seen from outside the modules: what was pushed where, and that every task succeeded. In
        // any order: the shipment can run and push its invoice before the shipment push is recorded.
        assertThat(tester.captured()).extracting(captured -> captured.topic().name()).containsExactlyInAnyOrder("orders", "shipments", "invoices");
        assertThat(tester.captured()).allSatisfy(captured ->
            assertThat(tester.find(captured.result().id()).orElseThrow().state()).isEqualTo("succeeded"));
    }
}
