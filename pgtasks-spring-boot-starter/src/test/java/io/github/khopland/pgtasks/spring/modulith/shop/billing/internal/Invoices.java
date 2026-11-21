package io.github.khopland.pgtasks.spring.modulith.shop.billing.internal;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.stereotype.Component;

@Component
public class Invoices {

    public final List<String> sent = new CopyOnWriteArrayList<>();
}
