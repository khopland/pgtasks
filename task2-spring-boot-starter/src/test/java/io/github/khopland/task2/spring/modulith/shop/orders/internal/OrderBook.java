package io.github.khopland.task2.spring.modulith.shop.orders.internal;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.stereotype.Component;

@Component
public class OrderBook {

    public final List<String> orders = new CopyOnWriteArrayList<>();
}
