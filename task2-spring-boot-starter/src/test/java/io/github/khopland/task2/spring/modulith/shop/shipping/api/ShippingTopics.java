package io.github.khopland.task2.spring.modulith.shop.shipping.api;

import io.github.khopland.task2.TopicRef;

public final class ShippingTopics {

    public static final TopicRef<String> SHIPMENTS = TopicRef.of("shipments", String.class);

    private ShippingTopics() {
    }
}
