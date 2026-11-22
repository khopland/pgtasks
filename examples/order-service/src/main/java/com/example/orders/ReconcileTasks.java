package com.example.orders;

import java.util.Objects;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.engine.Cron;
import io.github.khopland.pgtasks.engine.TaskConfigurer;

import com.example.orders.Topics.Reconcile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import static com.example.orders.Topics.RECONCILE;

/** Every minute, once across the cluster: counts orders that have been in flight for too long. */
@Configuration
class ReconcileTasks {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReconcileTasks.class);

    @Bean
    TaskConfigurer reconcile(JdbcTemplate jdbc, StagedWrites writes, @Value("${spring.application.name}-${server.port}") String node) {
        return tasks -> tasks
            .topic(RECONCILE, batch -> {
                StagedWrites.Staged staged = batch.resource(writes);
                return batch.decide(work -> {
                    Reconcile reconcile = Objects.requireNonNull(work.payload());
                    Integer stuck = jdbc.queryForObject("""
                        SELECT count(*) FROM orders
                        WHERE status NOT IN ('COMPLETED', 'CANCELLED') AND created_at < now() - make_interval(mins => ?)""",
                        Integer.class, reconcile.olderThanMinutes());
                    if (stuck != null && stuck > 0) {
                        LOGGER.warn("{} order(s) in flight for more than {} minutes", stuck, reconcile.olderThanMinutes());
                    }
                    staged.add(work.task(), "INSERT INTO reconcile_run (node, stuck) VALUES (?, ?)", node, stuck);
                    return Decision.success(String.valueOf(stuck));
                });
            }, options -> options.resources(writes))
            .recurring(RECONCILE, "every-minute", Cron.parse("* * * * *"), "reconcile", new Reconcile(5));
    }
}
