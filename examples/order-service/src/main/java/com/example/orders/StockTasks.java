package com.example.orders;

import java.util.Objects;
import java.util.Set;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Junction;
import io.github.khopland.pgtasks.engine.TaskConfigurer;

import com.example.orders.Topics.OrderRef;
import com.example.orders.Topics.ShipRequest;
import com.example.orders.Topics.Step;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.example.orders.Topics.SHIP;
import static com.example.orders.Topics.STOCK;
import static com.example.orders.Topics.paidMarker;
import static com.example.orders.Topics.reservedMarker;

/** Reserves stock in our own tables, committed with the task's completion. */
@Configuration
class StockTasks {

    static final String RESERVE = """
        WITH moved AS (
            INSERT INTO stock_movement (order_id, kind) VALUES (?, 'reserve') ON CONFLICT DO NOTHING RETURNING order_id
        )
        UPDATE stock s SET reserved = s.reserved + l.quantity
        FROM order_line l JOIN moved ON moved.order_id = l.order_id
        WHERE s.sku = l.sku""";

    @Bean
    TaskConfigurer stock(StagedWrites writes) {
        return tasks -> tasks.topic(STOCK, batch -> {
            StagedWrites.Staged staged = batch.resource(writes);
            return batch.decide(work -> {
                OrderRef order = Objects.requireNonNull(work.payload());
                String id = order.orderId();
                staged.touchesStock(work.task(), id);
                staged.add(work.task(), RESERVE, id);
                staged.add(work.task(), "UPDATE orders SET reserved_at = now(), updated_at = now() WHERE id = ?", id);
                work.push(SHIP, reservedMarker(id), new ShipRequest(id, order.customerId(), Step.RESERVED));
                work.junction(SHIP, Junction.Counts.ANY, Set.of(paidMarker(id), reservedMarker(id)));
                return Decision.success();
            });
        }, options -> options.resources(writes).concurrency(2).batchSize(50));
    }
}
