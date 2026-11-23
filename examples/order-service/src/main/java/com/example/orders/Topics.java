package com.example.orders;

import io.github.khopland.pgtasks.TopicRef;

/**
 * Every topic of the order flow:
 *
 * <pre>
 * POST /orders ──► orders.payment
 *              ├─► orders.stock
 *              └─► orders.ship (waits with later() until the order is paid and reserved)
 *                       └─► customer.notify (shipped; feedback request 30 s later)
 * declined payment ──► orders.ship (REPLACE: cancel, releases the stock) ──► customer.notify (declined)
 * </pre>
 */
public final class Topics {

    /** What the payment and stock steps need to know about an order. */
    public record OrderRef(String orderId, String customerId, long amountCents) {
    }

    public enum Step {
        /** The order's own shipping task: it waits until payment and stock are done. */
        SHIP,
        /** Payment was declined: release the stock and tell the customer. */
        CANCEL
    }

    public record ShipRequest(String orderId, String customerId, Step step) {
    }

    public record Notice(String orderId, String customerId, String kind) {
    }

    public record Reconcile(int olderThanMinutes) {
    }

    public static final TopicRef<OrderRef> PAYMENT = TopicRef.of("orders.payment", OrderRef.class);
    public static final TopicRef<OrderRef> STOCK = TopicRef.of("orders.stock", OrderRef.class);
    public static final TopicRef<ShipRequest> SHIP = TopicRef.of("orders.ship", ShipRequest.class);
    public static final TopicRef<Notice> NOTIFY = TopicRef.of("customer.notify", Notice.class);
    public static final TopicRef<Reconcile> RECONCILE = TopicRef.of("orders.reconcile", Reconcile.class);

    private Topics() {
    }
}
