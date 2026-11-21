package io.github.khopland.pgtasks.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.function.Function;

import io.github.khopland.pgtasks.Batch;
import io.github.khopland.pgtasks.BatchResource;
import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Decisions;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Junction;
import io.github.khopland.pgtasks.Push;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.Work;

final class DefaultBatch<T> implements Batch<T> {

    private final TopicRef<T> topic;
    private final List<Task<T>> tasks;
    private final Map<BatchResource<?>, Object> resources;
    /** Checks a staged push's or junction's target topic; see {@link DefaultTasks#checkTarget}. */
    private final BiConsumer<TopicRef<?>, Boolean> targets;

    /** Each task's own observation, opened around its decider; see {@link TaskObserver.BatchObservation#task}. */
    private final Function<Task<?>, TaskObserver.TaskObservation> observations;

    DefaultBatch(TopicRef<T> topic, List<Task<T>> tasks, Map<BatchResource<?>, Object> resources,
                 BiConsumer<TopicRef<?>, Boolean> targets,
                 Function<Task<?>, TaskObserver.TaskObservation> observations) {
        this.topic = topic;
        this.tasks = List.copyOf(tasks);
        this.resources = resources;
        this.targets = targets;
        this.observations = observations;
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
            decisions.decide(task, decideOne(topic.name(), decider, work, observations.apply(task)), work.pushes, work.junctions);
        }
        return decisions.build();
    }

    @Override
    public Decisions decideConcurrently(int parallelism, Decider<T> decider) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be at least 1");
        }
        List<StagedWork<T>> works = tasks.stream().map(task -> new StagedWork<>(task, targets)).toList();
        Semaphore running = new Semaphore(parallelism);
        ExecutorService executor = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("pgtasks-" + topic.name() + "-task-", 0).factory());
        try {
            List<Future<Decision>> futures = new ArrayList<>(works.size());
            for (StagedWork<T> work : works) {
                running.acquire();
                futures.add(executor.submit(() -> {
                    try {
                        return decideOne(topic.name(), decider, work, observations.apply(work.task));
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
                } catch (ExecutionException e) {
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

    /**
     * One task's decision: an exception or Error from the decider fails only this task. The task's
     * observation is current while the decider runs, and is closed when it returns.
     */
    private static <T> Decision decideOne(String topic, Decider<T> decider, StagedWork<T> work, TaskObserver.TaskObservation observation) {
        try (TaskObserver.Scope ignored = observation.open()) {
            // Each task's own thread under decideConcurrently: marked as a handler's too.
            return Objects.requireNonNull(HandlerThread.run(topic, () -> decider.decide(work)), "decider returned null");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CancellationException("Batch cancelled");
        } catch (VirtualMachineError e) {
            observation.error(e);
            throw e;
        } catch (Throwable e) {
            // An Error, such as NoClassDefFoundError, fails the task like an exception does.
            observation.error(e);
            return Decision.failed(e);
        } finally {
            observation.close();
        }
    }

    private static final class StagedWork<T> implements Work<T> {

        private final Task<T> task;
        private final BiConsumer<TopicRef<?>, Boolean> targets;
        private final List<Push<?>> pushes = new ArrayList<>();
        private final List<Junction> junctions = new ArrayList<>();

        StagedWork(Task<T> task, BiConsumer<TopicRef<?>, Boolean> targets) {
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
