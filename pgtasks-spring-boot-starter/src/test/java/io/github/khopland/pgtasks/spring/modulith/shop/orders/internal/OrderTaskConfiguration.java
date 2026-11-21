package io.github.khopland.pgtasks.spring.modulith.shop.orders.internal;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.engine.TaskConfigurer;
import io.github.khopland.pgtasks.spring.modulith.shop.orders.api.OrderTopics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class OrderTaskConfiguration {

    @Bean
    TaskConfigurer orderTasks(OrderBook book) {
        return tasks -> tasks.topic(OrderTopics.ORDERS, batch -> batch.decide(task -> {
            book.orders.add(task.payload());
            return Decision.success();
        }));
    }
}
