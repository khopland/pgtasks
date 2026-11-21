package io.github.khopland.task2.spring.modulith.shop.billing.internal;

import io.github.khopland.task2.Decision;
import io.github.khopland.task2.engine.TaskConfigurer;
import io.github.khopland.task2.spring.modulith.shop.billing.api.BillingTopics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class BillingTaskConfiguration {

    @Bean
    TaskConfigurer billingTasks(Invoices invoices) {
        return tasks -> tasks.topic(BillingTopics.INVOICES, batch -> batch.decide(task -> {
            invoices.sent.add(task.payload());
            return Decision.success();
        }));
    }
}
