package io.github.khopland.pgtasks.spring.modulith.shop.billing.internal;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.engine.TaskConfigurer;
import io.github.khopland.pgtasks.spring.modulith.shop.billing.api.BillingTopics;

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
