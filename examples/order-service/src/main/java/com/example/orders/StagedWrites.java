package com.example.orders;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import io.github.khopland.pgtasks.BatchResource;
import io.github.khopland.pgtasks.Decisions;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TopicRef;

import org.springframework.stereotype.Component;

/**
 * Writes to the order tables that must commit exactly when the task's completion commits: a
 * handler stages SQL per task, and the statements of the tasks whose final decision continues run
 * on the completion's connection.
 */
@Component
public class StagedWrites implements BatchResource<StagedWrites.Staged> {

    public record Statement(String sql, Object... parameters) {
    }

    /** One batch's staged statements, by task id. Deciders may run concurrently. */
    public static final class Staged {
        private final Map<Long, List<Statement>> byTask = new ConcurrentHashMap<>();
        private final Map<Long, String> stockOrders = new ConcurrentHashMap<>();

        /**
         * This task's statements change the stock rows of this order. Before any statement runs,
         * the batch locks all such rows in sku order, so two batches can't deadlock on them.
         */
        public void touchesStock(Task<?> task, String orderId) {
            stockOrders.put(task.id(), orderId);
        }

        public void add(Task<?> task, String sql, Object... parameters) {
            byTask.computeIfAbsent(task.id(), ignored -> new ArrayList<>()).add(new Statement(sql, parameters));
        }

        public List<Statement> statements(Task<?> task) {
            return byTask.getOrDefault(task.id(), List.of());
        }
    }

    @Override
    public Staged open(TopicRef<?> topic, List<Task<?>> tasks) {
        return new Staged();
    }

    @Override
    public void beforeCommit(Staged staged, Decisions finalDecisions, Connection connection) throws SQLException {
        String[] orders = finalDecisions.continuing().stream().map(task -> staged.stockOrders.get(task.id())).filter(Objects::nonNull)
            .toArray(String[]::new);
        if (orders.length > 0) {
            try (PreparedStatement lock = connection.prepareStatement("""
                SELECT 1 FROM stock WHERE sku IN (SELECT sku FROM order_line WHERE order_id = ANY (?)) ORDER BY sku FOR UPDATE""")) {
                lock.setArray(1, connection.createArrayOf("text", orders));
                lock.executeQuery().close();
            }
        }
        for (Task<?> task : finalDecisions.continuing()) {
            for (Statement statement : staged.statements(task)) {
                try (PreparedStatement prepared = connection.prepareStatement(statement.sql())) {
                    for (int i = 0; i < statement.parameters().length; i++) {
                        prepared.setObject(i + 1, statement.parameters()[i]);
                    }
                    prepared.executeUpdate();
                }
            }
        }
    }
}
