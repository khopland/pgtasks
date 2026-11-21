package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;

import com.zaxxer.hikari.HikariDataSource;
import io.github.khopland.pgtasks.postgres.PostgresNotifier;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.postgres.TaskSchema;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

/** Engines on a real Postgres. Each engine gets its own pool, so several engines stand in for several pods. */
public final class EngineFixture implements AutoCloseable {

    public static PostgresExtension extension() {
        return PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));
    }

    public final PostgresDatabase database;
    private final List<TaskEngine> engines = new CopyOnWriteArrayList<>();

    public EngineFixture(PostgresDatabase database) {
        this.database = database;
    }

    /** Fast housekeeping for tests; expiry stays long unless a test shortens it. */
    public static EngineSettings settings(String node) {
        return EngineSettings.defaults(node)
            .withHeartbeat(Duration.ofMillis(200), Duration.ofHours(1))
            .withPromoteInterval(Duration.ofMillis(50))
            // Tests that expire a node want its tasks back soon, not after 30 s.
            .withExpiries(new io.github.khopland.pgtasks.spi.TaskStore.Expiries(3, Duration.ofMillis(100)));
    }

    public TaskEngine start(String node, List<Topic<?>> topics) throws Exception {
        return start(node, topics, UnaryOperator.identity());
    }

    public TaskEngine start(String node, List<Topic<?>> topics, UnaryOperator<EngineSettings> customize) throws Exception {
        TaskEngine engine = create(node, topics, customize);
        engine.start();
        return engine;
    }

    public TaskEngine create(String node, List<Topic<?>> topics, UnaryOperator<EngineSettings> customize) {
        return create(node, topics, List.of(), customize);
    }

    public TaskEngine create(String node, List<Topic<?>> topics, List<TopicTemplate<?>> templates, UnaryOperator<EngineSettings> customize) {
        HikariDataSource pool = database.newPool(node, 12);
        EngineSettings settings = customize.apply(settings(node).withNotifier(new PostgresNotifier(database::connect)));
        TaskEngine engine = new TaskEngine(new PostgresTaskStore(pool), settings, topics, templates);
        engines.add(engine);
        return engine;
    }

    /** The engines this fixture created, in order. */
    public List<TaskEngine> engines() {
        return List.copyOf(engines);
    }

    public static void await(String what, Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out after " + timeout + " waiting for: " + what);
            }
            Thread.sleep(20);
        }
    }

    public void awaitQueueEmpty(Duration timeout) throws InterruptedException {
        await("an empty queue", timeout, () -> database.count("SELECT count(*) FROM tasks.queue") == 0);
    }

    public long count(String sql, Object... parameters) {
        return database.count(sql, parameters);
    }

    /** Creates topics up front, so a test can queue a whole batch before any engine polls. */
    public void createTopics(String... names) throws java.sql.SQLException {
        PostgresTaskStore store = new PostgresTaskStore(database.dataSource());
        try (io.github.khopland.pgtasks.spi.StoreTransaction transaction = store.begin()) {
            for (String name : names) {
                store.createTopic(transaction, name);
            }
            transaction.commit();
        }
    }

    /** Makes every delayed task and retry due now. */
    public void makeDue() {
        database.execute("UPDATE tasks.queue SET run_at = now() - interval '1 millisecond' WHERE run_at IS NOT NULL");
    }

    @Override
    public void close() {
        engines.forEach(engine -> engine.stop(Duration.ofSeconds(5)));
    }
}
