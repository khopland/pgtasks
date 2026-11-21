package org.example.task2.spring.modulith.shop.billing.api;

import org.example.task2.TopicRef;

public final class BillingTopics {

    public static final TopicRef<String> INVOICES = TopicRef.of("invoices", String.class);

    private BillingTopics() {
    }
}
