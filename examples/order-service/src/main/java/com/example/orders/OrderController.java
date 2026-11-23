package com.example.orders;

import java.util.List;
import java.util.Map;

import com.example.orders.OrderService.PlaceOrder;
import com.example.orders.OrderService.Placed;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orders;
    private final JdbcTemplate jdbc;

    public OrderController(OrderService orders, JdbcTemplate jdbc) {
        this.orders = orders;
        this.jdbc = jdbc;
    }

    @PostMapping
    public ResponseEntity<Placed> place(@RequestBody PlaceOrder request) {
        Placed placed = orders.place(request);
        return ResponseEntity.status(placed.created() ? HttpStatus.CREATED : HttpStatus.OK).body(placed);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Map<String, Object>> find(@PathVariable String id) {
        List<Map<String, Object>> rows = jdbc.queryForList("SELECT * FROM orders WHERE id = ?", id);
        return rows.isEmpty() ? ResponseEntity.notFound().build() : ResponseEntity.ok(rows.getFirst());
    }

    @GetMapping("/stats")
    public List<Map<String, Object>> stats() {
        return jdbc.queryForList("SELECT status, count(*) AS orders FROM orders GROUP BY status ORDER BY status");
    }
}
