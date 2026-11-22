package com.example.orders;

import java.util.Objects;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.engine.TaskConfigurer;

import com.example.orders.Partners.Declined;
import com.example.orders.Partners.Throttled;
import com.example.orders.Partners.Unavailable;
import com.example.orders.Topics.Notice;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.example.orders.Topics.NOTIFY;

/** Mails the customer, one message at a time per customer. */
@Configuration
class NotificationTasks {

    @Bean
    TaskConfigurer notifications(Partners partners, StagedWrites writes) {
        return tasks -> tasks.topic(NOTIFY, batch -> {
            StagedWrites.Staged staged = batch.resource(writes);
            return batch.decideConcurrently(16, work -> {
                Notice notice = Objects.requireNonNull(work.payload());
                try {
                    partners.notify(notice.orderId(), notice.customerId(), notice.kind(), work.idempotencyKey());
                } catch (Throttled throttled) {
                    return Decision.throttled(throttled.retryAfter());
                } catch (Unavailable unavailable) {
                    return Decision.unavailable(unavailable);
                } catch (Declined declined) {
                    return Decision.failed(declined);
                }
                staged.add(work.task(), """
                    INSERT INTO notification_log (order_id, kind) VALUES (?, ?)
                    ON CONFLICT (order_id, kind) DO UPDATE SET sent_count = notification_log.sent_count + 1""",
                    notice.orderId(), notice.kind());
                switch (notice.kind()) {
                    case "shipped" -> staged.add(work.task(),
                        "UPDATE orders SET status = 'COMPLETED', notified_at = now(), updated_at = now() WHERE id = ? AND status = 'SHIPPED'",
                        notice.orderId());
                    case "declined" -> staged.add(work.task(), "UPDATE orders SET notified_at = now(), updated_at = now() WHERE id = ?",
                        notice.orderId());
                    default -> {
                    }
                }
                return Decision.success();
            });
        }, options -> options
            .resources(writes)
            .limit("mailer")
            .ordering(Ordering.SINGULAR_BY_IDENTIFIER)
            .concurrency(2)
            .batchSize(32));
    }
}
