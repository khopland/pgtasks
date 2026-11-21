package io.github.khopland.task2.spring.modulith.shop.orders.internal;

import io.github.khopland.task2.Decision;
import io.github.khopland.task2.engine.TaskConfigurer;
import io.github.khopland.task2.spring.modulith.shop.orders.api.OrderTopics;
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
