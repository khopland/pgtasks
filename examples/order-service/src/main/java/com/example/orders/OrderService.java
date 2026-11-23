package com.example.orders;

import java.util.List;
import java.util.UUID;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Tasks;

import com.example.orders.Topics.OrderRef;
import com.example.orders.Topics.ShipRequest;
import com.example.orders.Topics.Step;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import static com.example.orders.Topics.PAYMENT;
import static com.example.orders.Topics.SHIP;
import static com.example.orders.Topics.STOCK;

@Service
public class OrderService {

    public record Line(String sku, int quantity) {
    }

    public record PlaceOrder(String requestId, String customerId, List<Line> lines) {
    }

    public record Placed(String orderId, boolean created) {
    }

    private final JdbcTemplate jdbc;
    private final Tasks tasks;

    public OrderService(JdbcTemplate jdbc, Tasks tasks) {
        this.jdbc = jdbc;
        this.tasks = tasks;
    }

    /** Saves the order and its three tasks in one transaction. A repeated request id returns the first order. */
    @Transactional
    public Placed place(PlaceOrder request) {
        if (request.lines().isEmpty()) {
            throw new IllegalArgumentException("an order needs at least one line");
        }
        String orderId = "o-" + UUID.randomUUID().toString().substring(0, 18);
        long amount = request.lines().stream().mapToLong(line -> line.quantity() * 1250L).sum();
        List<String> inserted = jdbc.queryForList("""
            INSERT INTO orders (id, request_id, customer_id, amount_cents, status) VALUES (?, ?, ?, ?, 'PLACED')
            ON CONFLICT (request_id) DO NOTHING RETURNING id
            """, String.class, orderId, request.requestId(), request.customerId(), amount);
        if (inserted.isEmpty()) {
            return new Placed(jdbc.queryForObject("SELECT id FROM orders WHERE request_id = ?", String.class, request.requestId()), false);
        }
        jdbc.batchUpdate("INSERT INTO order_line (order_id, sku, quantity) VALUES (?, ?, ?)",
            request.lines().stream().map(line -> new Object[] {orderId, line.sku(), line.quantity()}).toList());

        OrderRef ref = new OrderRef(orderId, request.customerId(), amount);
        tasks.push(PAYMENT, Insertion.APPEND, Creation.of(orderId, ref).withDedupKey("pay:" + orderId));
        tasks.push(STOCK, orderId, ref);
        tasks.push(SHIP, Insertion.APPEND, Creation.of(orderId, new ShipRequest(orderId, request.customerId(), Step.SHIP)));
        return new Placed(orderId, true);
    }
}
