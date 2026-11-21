package org.example.task2.engine;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.example.task2.Pushed;
import org.example.task2.Task;
import org.example.task2.TopicRef;

/** Wraps an observer so its failures are logged and never reach the engine's work. */
final class SafeObserver implements TaskObserver {

    private static final System.Logger LOGGER = System.getLogger(SafeObserver.class.getName());

    private final TaskObserver observer;

    private SafeObserver(TaskObserver observer) {
        this.observer = observer;
    }

    static TaskObserver of(TaskObserver observer) {
        return observer == TaskObserver.NONE || observer instanceof SafeObserver ? observer : new SafeObserver(observer);
    }

    @Override
    public PushObservation pushing(TopicRef<?> topic, int tasks) {
        PushObservation observation = call(() -> observer.pushing(topic, tasks), PushObservation.NONE);
        return new PushObservation() {
            @Override
            public String traceContext() {
                return call(observation::traceContext, null);
            }

            @Override
            public void close() {
                run(observation::close);
            }
        };
    }

    @Override
    public void breaker(TopicRef<?> topic, boolean open) {
        run(() -> observer.breaker(topic, open));
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

    private static <V> V call(Supplier<V> call, V fallback) {
        try {
            V value = call.get();
            return value == null ? fallback : value;
        } catch (RuntimeException e) {
            LOGGER.log(System.Logger.Level.WARNING, "A task observer failed", e);
            return fallback;
        }
    }

    private static void run(Runnable call) {
        try {
            call.run();
        } catch (RuntimeException e) {
            LOGGER.log(System.Logger.Level.WARNING, "A task observer failed", e);
        }
    }
}
