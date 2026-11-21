package org.example.task2.spring.modulith.shop.shipping.internal;

import org.example.task2.Creation;
import org.example.task2.Decision;
import org.example.task2.Insertion;
import org.example.task2.Push;
import org.example.task2.engine.TaskConfigurer;
import org.example.task2.spring.modulith.shop.billing.api.BillingTopics;
import org.example.task2.spring.modulith.shop.orders.api.OrderPlaced;
import org.example.task2.spring.modulith.shop.shipping.api.ShippingTopics;
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
