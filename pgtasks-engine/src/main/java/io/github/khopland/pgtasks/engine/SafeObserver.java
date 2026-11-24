package io.github.khopland.pgtasks.engine;

import java.util.LinkedHashMap;
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
                return call(() -> storable(observation.headers()), Map.of());
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

    /**
     * The headers a task can be stored and loaded with, as {@link io.github.khopland.pgtasks.Creation}
     * checks its own: a null or empty name and a null value are dropped, and a NUL character, which
     * Postgres can't store, becomes U+FFFD. A header a task can't hold would fail its push, or, for
     * downstream work, the completion of the whole batch that staged it, on every retry.
     */
    static Map<String, String> storable(@Nullable Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return Map.of();
        }
        boolean fine = true;
        for (Map.Entry<String, String> header : headers.entrySet()) {
            if (!storable(header.getKey()) || header.getValue() == null || header.getValue().indexOf('\0') >= 0) {
                fine = false;
                break;
            }
        }
        if (fine) {
            return headers;
        }
        Map<String, String> kept = new LinkedHashMap<>();
        headers.forEach((name, value) -> {
            if (name != null && !name.isEmpty() && value != null) {
                kept.putIfAbsent(name.replace('\0', REPLACEMENT), value.replace('\0', REPLACEMENT));
            } else {
                LOGGER.warn("A task observer gave a header with no name or no value, which is left out: {}={}", name, value);
            }
        });
        return kept;
    }

    private static final char REPLACEMENT = '\uFFFD';

    private static boolean storable(@Nullable String name) {
        return name != null && !name.isEmpty() && name.indexOf('\0') < 0;
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
