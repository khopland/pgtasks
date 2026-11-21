package io.github.khopland.task2.spring.modulith.shop.billing.api;

import io.github.khopland.task2.TopicRef;

public final class BillingTopics {

    public static final TopicRef<String> INVOICES = TopicRef.of("invoices", String.class);

    private BillingTopics() {
    }
}
