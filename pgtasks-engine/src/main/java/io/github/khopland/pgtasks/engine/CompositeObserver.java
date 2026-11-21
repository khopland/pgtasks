package io.github.khopland.pgtasks.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TopicRef;

import org.jspecify.annotations.Nullable;

/**
 * Several observers seen as one, each wrapped so one that throws doesn't keep the others from
 * seeing anything. A push's stored trace is the first observer's that gives one.
 */
final class CompositeObserver implements TaskObserver {

    private final List<TaskObserver> observers;

    CompositeObserver(List<TaskObserver> observers) {
        this.observers = observers.stream().map(SafeObserver::of).toList();
    }

    @Override
    public PushObservation pushing(TopicRef<?> topic, int tasks) {
        List<PushObservation> each = observers.stream().map(observer -> observer.pushing(topic, tasks)).toList();
        return new PushObservation() {
            @Override
            public @Nullable String traceContext() {
                return each.stream().map(PushObservation::traceContext).filter(java.util.Objects::nonNull).findFirst().orElse(null);
            }

            @Override
            public void close() {
                each.reversed().forEach(PushObservation::close);
            }
        };
    }

    @Override
    public void pushed(TopicRef<?> topic, List<Pushed> results) {
        observers.forEach(observer -> observer.pushed(topic, results));
    }

    @Override
    public void limitWait(TopicRef<?> topic, String limit, java.time.Duration wait) {
        observers.forEach(observer -> observer.limitWait(topic, limit, wait));
    }

    @Override
    public void breaker(TopicRef<?> topic, boolean open) {
        observers.forEach(observer -> observer.breaker(topic, open));
    }

    @Override
    public BatchObservation batch(TopicRef<?> topic, List<Task<?>> tasks) {
        List<BatchObservation> each = observers.stream().map(observer -> observer.batch(topic, tasks)).toList();
        return new BatchObservation() {
            @Override
            public Scope open() {
                List<Scope> scopes = new ArrayList<>(each.size());
                each.forEach(observation -> scopes.add(observation.open()));
                return () -> scopes.reversed().forEach(Scope::close);
            }

            @Override
            public void error(Throwable error) {
                each.forEach(observation -> observation.error(error));
            }

            @Override
            public void completed(Map<Result, Integer> results) {
                each.forEach(observation -> observation.completed(results));
            }

            @Override
            public void released() {
                each.forEach(BatchObservation::released);
            }

            @Override
            public void close() {
                each.reversed().forEach(BatchObservation::close);
            }
        };
    }
}
