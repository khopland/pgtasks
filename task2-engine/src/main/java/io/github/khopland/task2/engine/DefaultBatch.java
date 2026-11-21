package io.github.khopland.task2.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;

import io.github.khopland.task2.Batch;
import io.github.khopland.task2.BatchResource;
import io.github.khopland.task2.Creation;
import io.github.khopland.task2.Decision;
import io.github.khopland.task2.Decisions;
import io.github.khopland.task2.Insertion;
import io.github.khopland.task2.Junction;
import io.github.khopland.task2.Push;
import io.github.khopland.task2.Task;
import io.github.khopland.task2.TopicRef;
import io.github.khopland.task2.Work;

final class DefaultBatch<T> implements Batch<T> {

    private final TopicRef<T> topic;
    private final List<Task<T>> tasks;
    private final Map<BatchResource<?>, Object> resources;
    /** Checks a staged push's or junction's target topic; see {@link DefaultTasks#checkTarget}. */
    private final java.util.function.BiConsumer<TopicRef<?>, Boolean> targets;

    DefaultBatch(TopicRef<T> topic, List<Task<T>> tasks, Map<BatchResource<?>, Object> resources) {
        this(topic, tasks, resources, (target, codec) -> { });
    }

    DefaultBatch(TopicRef<T> topic, List<Task<T>> tasks, Map<BatchResource<?>, Object> resources,
                 java.util.function.BiConsumer<TopicRef<?>, Boolean> targets) {
        this.topic = topic;
        this.tasks = List.copyOf(tasks);
        this.resources = resources;
        this.targets = targets;
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
            StagedWork<T> work = new StagedWork<>(task, targets);
            decisions.decide(task, decideOne(topic.name(), decider, work), work.pushes, work.junctions);
        }
        return decisions.build();
    }

    @Override
    public Decisions decideConcurrently(int parallelism, Decider<T> decider) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be at least 1");
        }
        List<StagedWork<T>> works = tasks.stream().map(task -> new StagedWork<>(task, targets)).toList();
        java.util.concurrent.Semaphore running = new java.util.concurrent.Semaphore(parallelism);
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("task2-" + topic.name() + "-task-", 0).factory());
        try {
            List<java.util.concurrent.Future<Decision>> futures = new ArrayList<>(works.size());
            for (StagedWork<T> work : works) {
                running.acquire();
                futures.add(executor.submit(() -> {
                    try {
                        return decideOne(topic.name(), decider, work);
                    } finally {
                        running.release();
                    }
                }));
            }
            Decisions.Builder decisions = Decisions.builder();
            for (int index = 0; index < works.size(); index++) {
                StagedWork<T> work = works.get(index);
                Decision decision;
                try {
                    decision = futures.get(index).get();
                } catch (java.util.concurrent.ExecutionException e) {
                    // decideOne turns everything else into a decision: only a cancellation or a
                    // VirtualMachineError gets here, and the batch records the latter, then rethrows it.
                    if (e.getCause() instanceof VirtualMachineError fatal) {
                        throw fatal;
                    }
                    throw new CancellationException("Batch cancelled");
                }
                decisions.decide(work.task, decision, work.pushes, work.junctions);
            }
            return decisions.build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CancellationException("Batch cancelled");
        } finally {
            // Cancels what still runs when the batch is cancelled, and waits until it has ended:
            // a decider that ignores cancellation keeps the batch, and so its claims, until it
            // returns. Without the wait the task was retried while the old decider still ran.
            executor.shutdownNow();
            executor.close();
        }
    }

    /** One task's decision: an exception or Error from the decider fails only this task. */
    private static <T> Decision decideOne(String topic, Decider<T> decider, StagedWork<T> work) {
        try {
            // Each task's own thread under decideConcurrently: marked as a handler's too.
            return Objects.requireNonNull(HandlerThread.run(topic, () -> decider.decide(work)), "decider returned null");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CancellationException("Batch cancelled");
        } catch (VirtualMachineError e) {
            throw e;
        } catch (Throwable e) {
            // An Error, such as NoClassDefFoundError, fails the task like an exception does.
            return Decision.failed(e);
        }
    }

    private static final class StagedWork<T> implements Work<T> {

        private final Task<T> task;
        private final java.util.function.BiConsumer<TopicRef<?>, Boolean> targets;
        private final List<Push<?>> pushes = new ArrayList<>();
        private final List<Junction> junctions = new ArrayList<>();

        StagedWork(Task<T> task, java.util.function.BiConsumer<TopicRef<?>, Boolean> targets) {
            this.task = task;
            this.targets = targets;
        }

        @Override
        public Task<T> task() {
            return task;
        }

        @Override
        public <C> void push(TopicRef<C> topic, Insertion insertion, Creation<C> creation) {
            Push<C> push = new Push<>(topic, insertion, creation);
            targets.accept(topic, true);
            pushes.add(push);
        }

        @Override
        public void junction(TopicRef<?> topic, Junction.Counts counts, Set<String> identifiers) {
            Junction junction = new Junction(topic, counts, identifiers);
            targets.accept(topic, false);
            junctions.add(junction);
        }
    }
}
