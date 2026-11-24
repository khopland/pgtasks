package io.github.khopland.pgtasks.engine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TopicRef;

/**
 * Several observers seen as one, each wrapped so one that throws doesn't keep the others from
 * seeing anything. A push stores every observer's headers; for a name two give, the first one's.
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
            public Map<String, String> headers() {
                Map<String, String> headers = new LinkedHashMap<>();
                each.forEach(observation -> observation.headers().forEach(headers::putIfAbsent));
                return headers;
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
            public TaskObservation task(Task<?> task) {
                List<TaskObservation> tasks = each.stream().map(observation -> observation.task(task)).toList();
                return new TaskObservation() {
                    @Override
                    public Scope open() {
                        List<Scope> scopes = new ArrayList<>(tasks.size());
                        tasks.forEach(observation -> scopes.add(observation.open()));
                        return () -> scopes.reversed().forEach(Scope::close);
                    }

                    @Override
                    public void error(Throwable error) {
                        tasks.forEach(observation -> observation.error(error));
                    }

                    @Override
                    public void close() {
                        tasks.reversed().forEach(TaskObservation::close);
                    }
                };
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
