package io.github.khopland.task2.spring.modulith.shop.orders.api;

import io.github.khopland.task2.TopicRef;

public final class OrderTopics {

    public static final TopicRef<String> ORDERS = TopicRef.of("orders", String.class);

    private OrderTopics() {
    }
}
