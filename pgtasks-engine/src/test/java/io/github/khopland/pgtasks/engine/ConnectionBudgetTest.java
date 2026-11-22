package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import javax.sql.DataSource;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Review 2026-11-21: the notifier held one of the pool's connections for good, and 8 batches in
 * flight plus housekeeping could want more connections than Spring Boot's default pool of 10.
 */
class ConnectionBudgetTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static TaskEngineBuilder engine(DataSource dataSource) {
        return TaskEngine.builder().dataSource(dataSource)
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> Decision.success())));
    }

    @Test
    void batches_in_flight_default_to_half_the_pool_unless_set(PostgresDatabase database) throws Exception {
        try (TaskEngine small = engine(database.newPool("small", 6)).build();
             TaskEngine large = engine(database.newPool("large", 40)).build();
             TaskEngine set = engine(database.newPool("set", 6)).globals(globals -> globals.maxInFlightBatches(5)).build()) {
            assertThat(small.settings().maxInFlightBatches()).isEqualTo(3);
            assertThat(large.settings().maxInFlightBatches()).as("never above the default").isEqualTo(8);
            assertThat(set.settings().maxInFlightBatches()).as("set explicitly: kept, with a warning").isEqualTo(5);
        }
    }

    private static boolean listening(TaskEngine engine) {
        try {
            return engine.health().notifications() == EngineHealth.Notifications.CONNECTED;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void notifications_listen_outside_the_pool(PostgresDatabase database) throws Exception {
        HikariDataSource pool = database.newPool("app", 4);
        try (TaskEngine engine = engine(pool).build().start()) {
            EngineFixture.await("notifications", Duration.ofSeconds(5), () -> listening(engine));
            int fewest = Integer.MAX_VALUE;
            for (int sample = 0; sample < 20; sample++) {
                fewest = Math.min(fewest, pool.getHikariPoolMXBean().getActiveConnections());
                Thread.sleep(10);
            }
            assertThat(fewest).as("no pool connection held while idle").isZero();
            assertThat(database.count("SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'LISTEN pgtasks_%'")).isEqualTo(1);
        }
    }
}
