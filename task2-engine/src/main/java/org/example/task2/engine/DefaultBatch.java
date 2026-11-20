package org.example.task2.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;

import org.example.task2.Batch;
import org.example.task2.BatchResource;
import org.example.task2.Creation;
import org.example.task2.Decision;
import org.example.task2.Decisions;
import org.example.task2.Insertion;
import org.example.task2.Push;
import org.example.task2.Task;
import org.example.task2.TopicRef;
import org.example.task2.Work;

final class DefaultBatch<T> implements Batch<T> {

    private final TopicRef<T> topic;
    private final List<Task<T>> tasks;
    private final Map<BatchResource<?>, Object> resources;

    DefaultBatch(TopicRef<T> topic, List<Task<T>> tasks, Map<BatchResource<?>, Object> resources) {
        this.topic = topic;
        this.tasks = List.copyOf(tasks);
        this.resources = resources;
    }

    @Override
    public TopicRef<T> topic() {
        return topic;
    }

    @Override
    public List<Task<T>> tasks() {
        return tasks;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R resource(BatchResource<R> resource) {
        if (!resources.containsKey(resource)) {
            throw new IllegalArgumentException("Resource " + resource + " is not registered for topic " + topic);
        }
        return (R) resources.get(resource);
    }

    @Override
    public Decisions decide(Decider<T> decider) {
        Decisions.Builder decisions = Decisions.builder();
        for (Task<T> task : tasks) {
            if (Thread.currentThread().isInterrupted()) {
                throw new CancellationException("Batch cancelled");
            }
            StagedWork<T> work = new StagedWork<>(task);
            Decision decision;
            try {
                decision = Objects.requireNonNull(decider.decide(work), "decider returned null");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CancellationException("Batch cancelled");
            } catch (Exception e) {
                decision = Decision.failed(e);
            }
            decisions.decide(task, decision, work.pushes);
        }
        return decisions.build();
    }

    private static final class StagedWork<T> implements Work<T> {

        private final Task<T> task;
        private final List<Push<?>> pushes = new ArrayList<>();

        StagedWork(Task<T> task) {
            this.task = task;
        }

        @Override
        public Task<T> task() {
            return task;
        }

        @Override
        public <C> void push(TopicRef<C> topic, Insertion insertion, Creation<C> creation) {
            pushes.add(new Push<>(topic, insertion, creation));
        }
    }
}
