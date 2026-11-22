package com.example.orders;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Junction;
import io.github.khopland.pgtasks.engine.Retry;
import io.github.khopland.pgtasks.engine.TaskConfigurer;

import com.example.orders.Partners.Declined;
import com.example.orders.Partners.Throttled;
import com.example.orders.Partners.Unavailable;
import com.example.orders.Topics.OrderRef;
import com.example.orders.Topics.ShipRequest;
import com.example.orders.Topics.Step;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.example.orders.Topics.PAYMENT;
import static com.example.orders.Topics.SHIP;
import static com.example.orders.Topics.paidMarker;
import static com.example.orders.Topics.reservedMarker;

/** Charges the order at the gateway. One remote call per task, so the batch decides concurrently. */
@Configuration
class PaymentTasks {

    @Bean
    TaskConfigurer payments(Partners partners, StagedWrites writes) {
        return tasks -> tasks.topic(PAYMENT, batch -> {
            StagedWrites.Staged staged = batch.resource(writes);
            return batch.decideConcurrently(16, work -> {
                OrderRef order = Objects.requireNonNull(work.payload());
                String id = order.orderId();
                try {
                    String reference = partners.pay(id, order.amountCents(), work.idempotencyKey());
                    staged.add(work.task(), """
                        UPDATE orders SET status = CASE WHEN status = 'PLACED' THEN 'PAID' ELSE status END,
                               payment_ref = ?, paid_at = now(), updated_at = now()
                        WHERE id = ?""", reference, id);
                    // Fan-in: the marker plus a junction over both markers and the order's own suspended ship task.
                    work.push(SHIP, paidMarker(id), new ShipRequest(id, order.customerId(), Step.PAID));
                    work.junction(SHIP, Junction.Counts.ANY, Set.of(paidMarker(id), reservedMarker(id)));
                    return Decision.success(reference);
                } catch (Declined declined) {
                    staged.add(work.task(), "UPDATE orders SET status = 'DECLINED', updated_at = now() WHERE id = ?", id);
                    // Replaces the suspended ship task: it will never be released now.
                    work.push(SHIP, Insertion.REPLACE, Creation.of(id, new ShipRequest(id, order.customerId(), Step.CANCEL)));
                    return Decision.success("declined");
                } catch (Throttled throttled) {
                    return Decision.throttled(throttled.retryAfter());
                } catch (Unavailable unavailable) {
                    return Decision.unavailable(unavailable);
                }
            });
        }, options -> options
            .resources(writes)
            .limit("gateway")
            .concurrency(2)
            .batchSize(32)
            .retry(Retry.standard().withBackoff(Duration.ofSeconds(2), Duration.ofMinutes(1))));
    }
}
