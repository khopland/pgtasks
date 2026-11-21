package io.github.khopland.pgtasks.spring.modulith.shop.billing.api;

import io.github.khopland.pgtasks.TopicRef;

public final class BillingTopics {

    public static final TopicRef<String> INVOICES = TopicRef.of("invoices", String.class);

    private BillingTopics() {
    }
}
