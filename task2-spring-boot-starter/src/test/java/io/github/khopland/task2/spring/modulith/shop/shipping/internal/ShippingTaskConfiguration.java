package io.github.khopland.task2.spring.modulith.shop.shipping.internal;

import io.github.khopland.task2.Creation;
import io.github.khopland.task2.Decision;
import io.github.khopland.task2.Insertion;
import io.github.khopland.task2.Push;
import io.github.khopland.task2.engine.TaskConfigurer;
import io.github.khopland.task2.spring.modulith.shop.billing.api.BillingTopics;
import io.github.khopland.task2.spring.modulith.shop.orders.api.OrderPlaced;
import io.github.khopland.task2.spring.modulith.shop.shipping.api.ShippingTopics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Ships every placed order, then invoices it: a push to the billing module's topic. */
@Configuration(proxyBeanMethods = false)
class ShippingTaskConfiguration {

    @Bean
    TaskConfigurer shippingTasks(Dispatch dispatch) {
        return tasks -> tasks
            .onEvent(OrderPlaced.class, placed -> new Push<>(ShippingTopics.SHIPMENTS, Insertion.APPEND, Creation.of(placed.id(), placed.id())))
            .topic(ShippingTopics.SHIPMENTS, batch -> batch.decide(task -> {
                dispatch.shipped.add(task.payload());
                task.push(BillingTopics.INVOICES, task.identifier(), "invoice for " + task.payload());
                return Decision.success();
            }));
    }
}
