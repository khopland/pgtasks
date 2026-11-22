package io.github.khopland.pgtasks.engine;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TopicRef;

import org.jspecify.annotations.Nullable;
import org.slf4j.MDC;

/** MDC keys stored as headers when a task is pushed, and put back while it is handled: see {@link TaskObserver#mdc}. */
final class MdcHeaders implements TaskObserver {

    private final List<String> keys;

    MdcHeaders(Collection<String> keys) {
        Objects.requireNonNull(keys, "keys");
        for (String key : keys) {
            if (Objects.requireNonNull(key, "key").isEmpty()) {
                throw new IllegalArgumentException("MDC keys must not be empty");
            }
        }
        this.keys = List.copyOf(new LinkedHashSet<>(keys));
    }

    @Override
    public PushObservation pushing(TopicRef<?> topic, int tasks) {
        Map<String, String> headers = new LinkedHashMap<>();
        for (String key : keys) {
            String value = MDC.get(key);
            if (value != null) {
                headers.put(key, value);
            }
        }
        Map<String, String> captured = Collections.unmodifiableMap(headers);
        return new PushObservation() {
            @Override
            public Map<String, String> headers() {
                return captured;
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public BatchObservation batch(TopicRef<?> topic, List<Task<?>> tasks) {
        Map<String, String> shared = shared(tasks);
        return new BatchObservation() {
            @Override
            public Scope open() {
                return put(shared);
            }

            @Override
            public TaskObservation task(Task<?> task) {
                return new TaskObservation() {
                    @Override
                    public Scope open() {
                        return put(task.headers());
                    }
                };
            }
        };
    }

    /** The values of the keys every task of the batch has, all with the same value. */
    private Map<String, String> shared(List<Task<?>> tasks) {
        Map<String, String> shared = new HashMap<>();
        if (tasks.isEmpty()) {
            return shared;
        }
        for (String key : keys) {
            String value = tasks.getFirst().headers().get(key);
            if (value != null && tasks.stream().allMatch(task -> value.equals(task.headers().get(key)))) {
                shared.put(key, value);
            }
        }
        return shared;
    }

    /** Sets each key to its value in {@code values}, or removes it, until the scope closes. */
    private Scope put(Map<String, String> values) {
        Map<String, @Nullable String> previous = new HashMap<>();
        for (String key : keys) {
            previous.put(key, MDC.get(key));
            String value = values.get(key);
            if (value == null) {
                MDC.remove(key);
            } else {
                MDC.put(key, value);
            }
        }
        return () -> previous.forEach((key, value) -> {
            if (value == null) {
                MDC.remove(key);
            } else {
                MDC.put(key, value);
            }
        });
    }
}
