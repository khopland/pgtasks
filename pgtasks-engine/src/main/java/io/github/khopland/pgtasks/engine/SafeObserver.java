package io.github.khopland.pgtasks.engine;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TopicRef;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Wraps an observer so its failures are logged and never reach the engine's work. */
final class SafeObserver implements TaskObserver {

    private static final Logger LOGGER = LoggerFactory.getLogger(SafeObserver.class);

    private final TaskObserver observer;

    private SafeObserver(TaskObserver observer) {
        this.observer = observer;
    }

    @SuppressWarnings("ReferenceEquality") // NONE is the one no-op instance
    static TaskObserver of(TaskObserver observer) {
        return observer == TaskObserver.NONE || observer instanceof SafeObserver ? observer : new SafeObserver(observer);
    }

    @Override
    public PushObservation pushing(TopicRef<?> topic, int tasks) {
        PushObservation observation = call(() -> observer.pushing(topic, tasks), PushObservation.NONE);
        return new PushObservation() {
            @Override
            public Map<String, String> headers() {
                return call(observation::headers, Map.of());
            }

            @Override
            public void close() {
                run(observation::close);
            }
        };
    }

    @Override
    public void pushed(TopicRef<?> topic, List<Pushed> results) {
        run(() -> observer.pushed(topic, results));
    }

    @Override
    public BatchObservation batch(TopicRef<?> topic, List<Task<?>> tasks) {
        BatchObservation observation = call(() -> observer.batch(topic, tasks), BatchObservation.NONE);
        return new BatchObservation() {
            @Override
            public Scope open() {
                Scope scope = call(observation::open, Scope.NONE);
                return () -> run(scope::close);
            }

            @Override
            public TaskObservation task(Task<?> task) {
                TaskObservation each = call(() -> observation.task(task), TaskObservation.NONE);
                return new TaskObservation() {
                    @Override
                    public Scope open() {
                        Scope scope = call(each::open, Scope.NONE);
                        return () -> run(scope::close);
                    }

                    @Override
                    public void error(Throwable error) {
                        run(() -> each.error(error));
                    }

                    @Override
                    public void close() {
                        run(each::close);
                    }
                };
            }

            @Override
            public void error(Throwable error) {
                run(() -> observation.error(error));
            }

            @Override
            public void completed(Map<Result, Integer> results) {
                run(() -> observation.completed(results));
            }

            @Override
            public void released() {
                run(observation::released);
            }

            @Override
            public void close() {
                run(observation::close);
            }
        };
    }

    private static <V extends @Nullable Object> V call(Supplier<V> call, V fallback) {
        try {
            V value = call.get();
            return value == null ? fallback : value;
        } catch (RuntimeException e) {
            LOGGER.warn("A task observer failed", e);
            return fallback;
        }
    }

    private static void run(Runnable call) {
        try {
            call.run();
        } catch (RuntimeException e) {
            LOGGER.warn("A task observer failed", e);
        }
    }
}
