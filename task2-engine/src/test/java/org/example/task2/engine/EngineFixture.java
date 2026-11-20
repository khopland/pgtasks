package org.example.task2.engine;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;

import com.zaxxer.hikari.HikariDataSource;
import org.example.task2.postgres.PostgresNotifier;
import org.example.task2.postgres.PostgresTaskStore;
import org.example.task2.postgres.TaskSchema;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;

/** Engines on a real Postgres. Each engine gets its own pool, so several engines stand in for several pods. */
final class EngineFixture implements AutoCloseable {

    static PostgresExtension extension() {
        return PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));
    }

    final PostgresDatabase database;
    private final List<TaskEngine> engines = new CopyOnWriteArrayList<>();

    EngineFixture(PostgresDatabase database) {
        this.database = database;
    }

    /** Fast housekeeping for tests; expiry stays long unless a test shortens it. */
    static EngineSettings settings(String node) {
        return EngineSettings.defaults(node)
            .withHeartbeat(Duration.ofMillis(200), Duration.ofHours(1))
            .withPromoteInterval(Duration.ofMillis(50));
    }

    TaskEngine start(String node, List<Topic<?>> topics) throws Exception {
        return start(node, topics, UnaryOperator.identity());
    }

    TaskEngine start(String node, List<Topic<?>> topics, UnaryOperator<EngineSettings> customize) throws Exception {
        TaskEngine engine = create(node, topics, customize);
        engine.start();
        return engine;
    }

    TaskEngine create(String node, List<Topic<?>> topics, UnaryOperator<EngineSettings> customize) {
        HikariDataSource pool = database.newPool(node, 12);
        EngineSettings settings = customize.apply(settings(node).withNotifier(new PostgresNotifier(database::connect)));
        TaskEngine engine = new TaskEngine(pool, new PostgresTaskStore(), settings, topics);
        engines.add(engine);
        return engine;
    }

    static void await(String what, Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out after " + timeout + " waiting for: " + what);
            }
            Thread.sleep(20);
        }
    }

    void awaitQueueEmpty(Duration timeout) throws InterruptedException {
        await("an empty queue", timeout, () -> database.count("SELECT count(*) FROM tasks.queue") == 0);
    }

    long count(String sql, Object... parameters) {
        return database.count(sql, parameters);
    }

    @Override
    public void close() {
        engines.forEach(engine -> engine.stop(Duration.ofSeconds(5)));
    }
}
