package io.github.khopland.pgtasks.spring.modulith.shop.orders;

import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.spring.modulith.shop.orders.api.OrderPlaced;
import io.github.khopland.pgtasks.spring.modulith.shop.orders.api.OrderTopics;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final Tasks tasks;
    private final ApplicationEventPublisher events;

    OrderService(Tasks tasks, ApplicationEventPublisher events) {
        this.tasks = tasks;
        this.events = events;
    }

    public void place(String id) {
        tasks.independently().push(OrderTopics.ORDERS, id, id);
        events.publishEvent(new OrderPlaced(id));
    }
}
