package io.github.khopland.task2.spring.modulith.shop.shipping.internal;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.stereotype.Component;

@Component
public class Dispatch {

    public final List<String> shipped = new CopyOnWriteArrayList<>();
}
