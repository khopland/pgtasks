package io.github.khopland.pgtasks.spring.modulith.shop.orders.api;

import io.github.khopland.pgtasks.TopicRef;

public final class OrderTopics {

    public static final TopicRef<String> ORDERS = TopicRef.of("orders", String.class);

    private OrderTopics() {
    }
}
