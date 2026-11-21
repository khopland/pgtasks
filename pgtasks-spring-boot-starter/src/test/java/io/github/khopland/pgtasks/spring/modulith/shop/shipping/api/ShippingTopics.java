package io.github.khopland.pgtasks.spring.modulith.shop.shipping.api;

import io.github.khopland.pgtasks.TopicRef;

public final class ShippingTopics {

    public static final TopicRef<String> SHIPMENTS = TopicRef.of("shipments", String.class);

    private ShippingTopics() {
    }
}
