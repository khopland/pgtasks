package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.UnaryOperator;

import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.ManualClock;
import io.github.khopland.pgtasks.spi.StoreTransaction;

/** Engines sharing one in-memory store with a manual clock: several nodes in one JVM, no database. */
final class MemoryEngines implements AutoCloseable {

    final ManualClock clock = ManualClock.startingNow();
    final InMemoryTaskStore store = new InMemoryTaskStore(clock);
    private final List<TaskEngine> engines = new CopyOnWriteArrayList<>();

    static EngineSettings settings(String node) {
        return EngineSettings.defaults(node).withHeartbeat(Duration.ofMillis(50), Duration.ofMinutes(10)).withPromoteInterval(Duration.ofMillis(20));
    }

    TaskEngine start(String node, List<Topic<?>> topics) throws Exception {
        return start(node, topics, UnaryOperator.identity());
    }

    TaskEngine start(String node, List<Topic<?>> topics, UnaryOperator<EngineSettings> customize) throws Exception {
        return create(node, topics, customize).start();
    }

    TaskEngine create(String node, List<Topic<?>> topics, UnaryOperator<EngineSettings> customize) {
        TaskEngine engine = new TaskEngine(store, customize.apply(settings(node).withNotifier(store.notifier())), topics);
        engines.add(engine);
        return engine;
    }

    /** An engine built elsewhere, stopped with the others. */
    TaskEngine add(TaskEngine engine) {
        engines.add(engine);
        return engine;
    }

    TaskTester tester() {
        return TaskTester.of(engines);
    }

    void createTopics(String... names) throws Exception {
        try (StoreTransaction transaction = store.begin()) {
            for (String name : names) {
                store.createTopic(transaction, name);
            }
            transaction.commit();
        }
    }

    @Override
    public void close() {
        engines.forEach(engine -> engine.stop(Duration.ofSeconds(5)));
    }
}
