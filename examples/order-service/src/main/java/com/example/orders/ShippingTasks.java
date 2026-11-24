package com.example.orders;

import java.time.Duration;
import java.util.Objects;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.engine.Retry;
import io.github.khopland.pgtasks.engine.TaskConfigurer;

import com.example.orders.Partners.Declined;
import com.example.orders.Partners.Rejected;
import com.example.orders.Partners.Throttled;
import com.example.orders.Partners.Unavailable;
import com.example.orders.Topics.Notice;
import com.example.orders.Topics.ShipRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import static com.example.orders.Topics.NOTIFY;
import static com.example.orders.Topics.SHIP;

/** Books the shipment once payment and stock are done, or cancels a declined order. */
@Configuration
class ShippingTasks {

    static final String RELEASE = """
        WITH moved AS (
            INSERT INTO stock_movement (order_id, kind) VALUES (?, 'release') ON CONFLICT DO NOTHING RETURNING order_id
        )
        UPDATE stock s SET reserved = s.reserved - l.quantity
        FROM order_line l JOIN moved ON moved.order_id = l.order_id
        WHERE s.sku = l.sku""";

    /** Where an order is: what the shipping step waits for. */
    interface OrderStates {
        /** The stock step has reserved its stock. */
        boolean reserved(String orderId);

        /** The payment step has charged it. */
        boolean paid(String orderId);

        /** Its payment was declined: it will never ship. */
        boolean declined(String orderId);
    }

    @Bean
    OrderStates orderStates(JdbcTemplate jdbc) {
        return new OrderStates() {
            @Override
            public boolean reserved(String orderId) {
                return Boolean.TRUE.equals(jdbc.queryForObject(
                    "SELECT EXISTS (SELECT 1 FROM stock_movement WHERE order_id = ? AND kind = 'reserve')", Boolean.class, orderId));
            }

            @Override
            public boolean paid(String orderId) {
                return Boolean.TRUE.equals(jdbc.queryForObject("SELECT paid_at IS NOT NULL FROM orders WHERE id = ?", Boolean.class, orderId));
            }

            @Override
            public boolean declined(String orderId) {
                return Boolean.TRUE.equals(jdbc.queryForObject("SELECT status IN ('DECLINED', 'CANCELLED') FROM orders WHERE id = ?",
                    Boolean.class, orderId));
            }
        };
    }

    @Bean
    TaskConfigurer shipping(Partners partners, StagedWrites writes, OrderStates orders) {
        return tasks -> tasks.topic(SHIP, batch -> {
            StagedWrites.Staged staged = batch.resource(writes);
            return batch.decideConcurrently(16, work -> {
                ShipRequest request = Objects.requireNonNull(work.payload());
                String id = request.orderId();
                switch (request.step()) {
                    case CANCEL -> {
                        if (!orders.reserved(id)) {
                            return Decision.later(Duration.ofSeconds(5));   // the stock step hasn't run yet
                        }
                        staged.touchesStock(work.task(), id);
                        staged.add(work.task(), RELEASE, id);
                        staged.add(work.task(), "UPDATE orders SET status = 'CANCELLED', updated_at = now() WHERE id = ?", id);
                        work.push(NOTIFY, Insertion.APPEND, Creation.of(request.customerId(), new Notice(id, request.customerId(), "declined"))
                            .withDedupKey("notify:" + id + ":declined"));
                        return Decision.success();
                    }
                    case SHIP -> {
                        // Fan-in: the order ships once payment and stock are both done.
                        if (orders.declined(id)) {
                            return Decision.filtered("payment declined");
                        }
                        if (!orders.paid(id) || !orders.reserved(id)) {
                            return Decision.later(Duration.ofSeconds(2));
                        }
                        try {
                            String tracking = partners.ship(id, work.idempotencyKey());
                            staged.add(work.task(), "UPDATE orders SET status = 'SHIPPED', tracking = ?, shipped_at = now(), updated_at = now() WHERE id = ?",
                                tracking, id);
                            work.push(NOTIFY, Insertion.APPEND, Creation.of(request.customerId(), new Notice(id, request.customerId(), "shipped"))
                                .withDedupKey("notify:" + id + ":shipped"));
                            work.push(NOTIFY, Insertion.APPEND, Creation.of(request.customerId(), new Notice(id, request.customerId(), "feedback"))
                                .withDelay(Duration.ofSeconds(30))
                                .withDedupKey("notify:" + id + ":feedback"));
                            return Decision.success(tracking);
                        } catch (Throttled throttled) {
                            return Decision.later(throttled.retryAfter());
                        } catch (Unavailable unavailable) {
                            return Decision.failed(unavailable);
                        } catch (Declined | Rejected refused) {
                            return Decision.failed(refused);
                        }
                    }
                    default -> throw new IllegalStateException("unknown step " + request.step());
                }
            });
        }, options -> options
            .resources(writes)
            .retry(Retry.standard().attempts(3).withBackoff(Duration.ofSeconds(2), Duration.ofSeconds(10)).notCounting(Unavailable.class))
            .concurrency(2)
            .batchSize(32));
    }
}
