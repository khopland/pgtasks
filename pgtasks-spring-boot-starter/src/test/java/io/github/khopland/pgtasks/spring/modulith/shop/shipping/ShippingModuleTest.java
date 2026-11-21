package io.github.khopland.pgtasks.spring.modulith.shop.shipping;

import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.spring.modulith.shop.billing.api.BillingTopics;
import io.github.khopland.pgtasks.spring.modulith.shop.orders.api.OrderPlaced;
import io.github.khopland.pgtasks.spring.modulith.shop.shipping.internal.Dispatch;

import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.modulith.test.ApplicationModuleTest;
import org.springframework.test.context.TestConstructor;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shipping module alone: only its topic runs, the event from orders arrives through the
 * bridge, and the push to billing's topic is kept for the assertion instead of running.
 */
@ApplicationModuleTest
@TestPropertySource(properties = "pgtasks.store=memory")
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
class ShippingModuleTest {

    private final ApplicationEventPublisher events;
    private final TaskTester tester;
    private final TaskEngine engine;
    private final Dispatch dispatch;

    ShippingModuleTest(ApplicationEventPublisher events, TaskTester tester, TaskEngine engine, Dispatch dispatch) {
        this.events = events;
        this.tester = tester;
        this.engine = engine;
        this.dispatch = dispatch;
    }

    @Test
    void a_placed_order_is_shipped_and_its_invoice_pushed_to_billing() {
        events.publishEvent(new OrderPlaced("o-7"));
        tester.awaitIdle();

        assertThat(engine.processing()).containsExactly("shipments");
        assertThat(dispatch.shipped).containsExactly("o-7");
        assertThat(tester.pushed(BillingTopics.INVOICES)).singleElement()
            .satisfies(invoice -> assertThat(invoice.payload()).isEqualTo("invoice for o-7"));
        assertThat(tester.counts(BillingTopics.INVOICES).ready()).as("billing doesn't run here").isEqualTo(1);
    }
}
