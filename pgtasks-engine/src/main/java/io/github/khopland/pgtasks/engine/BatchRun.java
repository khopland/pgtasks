package io.github.khopland.pgtasks.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import io.github.khopland.pgtasks.BatchResource;
import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Decisions;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Junction;
import io.github.khopland.pgtasks.Push;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.spi.StaleClaimException;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore.ClaimedTask;
import io.github.khopland.pgtasks.spi.TaskStore.Completion;
import io.github.khopland.pgtasks.spi.TaskStore.Outcome;
import io.github.khopland.pgtasks.spi.TaskStore.Resolution;
import io.github.khopland.pgtasks.spi.TaskStore.WaitState;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One claimed batch, from decoding to completion:
 *
 * <ol>
 *   <li>open the topic's batch resources,</li>
 *   <li>run the handler in the batch scope, bounded by the batch deadline,</li>
 *   <li>let every resource review the handler's decisions, and merge them: the most severe wins,</li>
 *   <li>in one transaction: the fenced completion, the downstream work of continuing tasks and
 *       every resource's JDBC effects, all based on the same final decisions,</li>
 *   <li>after the commit: external effects and wake-ups.</li>
 * </ol>
 *
 * If the completion transaction fails, nothing of it is kept and the batch's tasks are
 * recorded as failed in a new transaction. If it found a stale claim, the tasks still owned
 * are released without counting the attempt.
 */
final class BatchRun<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchRun.class);

    private static final Resolution RELEASE = new Resolution.Waiting(WaitState.RETRYING, Duration.ZERO, false, null);

    private final TopicWorker<T> worker;
    private final EngineCore core;
    private final List<ClaimedTask> claimed;
    private @Nullable BatchScope scope;
    /** A VirtualMachineError from the handler: recorded as the batch's failure, then rethrown. */
    private @Nullable VirtualMachineError fatal;
    private TaskObserver.BatchObservation batchObservation = TaskObserver.BatchObservation.NONE;
    /** Each task's own observation, by task id, created the first time the engine needs it. */
    private final Map<Long, TaskObserver.TaskObservation> taskObservations = new ConcurrentHashMap<>();

    BatchRun(TopicWorker<T> worker, EngineCore core, List<ClaimedTask> claimed) {
        this.worker = worker;
        this.core = core;
        this.claimed = claimed;
    }

    void run() {
        Topic<T> topic = worker.topic();
        Map<Long, ClaimedTask> claims = new LinkedHashMap<>();
        List<Task<T>> runnable = new ArrayList<>();
        List<Task<?>> all = new ArrayList<>(claimed.size());
        Decisions.Builder early = Decisions.builder();
        for (ClaimedTask claim : claimed) {
            claims.put(claim.id(), claim);
            @Nullable T payload;
            try {
                payload = claim.payload() == null ? null : topic.codec().decode(claim.payload());
            } catch (RuntimeException | LinkageError e) {
                Task<T> undecoded = task(claim, null);
                all.add(undecoded);
                early.decide(undecoded, Decision.failed(new UndecodablePayload(claim.payloadType(), e)));
                continue;
            }
            Task<T> task = task(claim, payload);
            all.add(task);
            if (claim.attempt() > topic.settings().retry().maxAttempts()) {
                // The last attempt expired with its node, or the policy now allows fewer than it did.
                early.decide(task, Decision.failed("No attempts left: this is attempt " + claim.attempt() + ", the retry policy allows "
                    + topic.settings().retry().maxAttempts()));
            } else {
                runnable.add(task);
            }
        }
        Map<BatchResource<?>, Object> opened = new LinkedHashMap<>();
        TaskObserver.BatchObservation observation = core.observer().batch(topic.ref(), all);
        this.batchObservation = observation;
        try {
            runBatch(topic, claims, runnable, early.build(), opened, observation);
        } finally {
            opened.values().forEach(BatchRun::closeQuietly);
            if (scope == null || !scope.abandoned()) {
                // Abandoned claims stay held, or the stop's orphan release would hand them back.
                claimed.forEach(claim -> core.held().remove(claim.id(), claim.attempt()));
            }
            taskObservations.values().forEach(TaskObserver.TaskObservation::close);
            observation.close();
        }
        if (fatal != null) {
            throw fatal;
        }
    }

    private void runBatch(Topic<T> topic, Map<Long, ClaimedTask> claims, List<Task<T>> runnable, Decisions early, Map<BatchResource<?>, Object> opened,
                          TaskObserver.BatchObservation observation) {
        Decisions decisions = runnable.isEmpty() ? Decisions.builder().build() : process(topic, runnable, opened, observation);
        if (scope != null && scope.abandoned()) {
            // The handler still runs: handing its tasks back would let them run twice at once.
            observation.released();
            return;
        }
        if (decisions == null) {
            release(claims, claimed.stream().map(ClaimedTask::id).toList());
            observation.released();
            return;
        }
        try (TaskObserver.Scope ignored = observation.open()) {
            complete(topic, claims, decisions, early, opened, observation);
        }
    }

    /** Hands every claimed task back without running it or counting the attempt. */
    void releaseAll() {
        Map<Long, ClaimedTask> claims = new LinkedHashMap<>();
        claimed.forEach(claim -> claims.put(claim.id(), claim));
        release(claims, List.copyOf(claims.keySet()));
        claimed.forEach(claim -> core.held().remove(claim.id(), claim.attempt()));
    }

    static final Resolution RELEASED = RELEASE;

    private Task<T> task(ClaimedTask claim, @Nullable T payload) {
        return new Task<>(worker.topic().ref(), claim.id(), claim.identifier(), payload, claim.attempt(), claim.reference(),
            claim.parentId(), claim.rootId(), claim.createdAt(), claim.claimedAt(), claim.headers());
    }

    /** The task's own observation, asked for once; see {@link TaskObserver.BatchObservation#task}. */
    private TaskObserver.TaskObservation observe(Task<?> task) {
        return taskObservations.computeIfAbsent(task.id(), id -> new ClosedOnce(batchObservation.task(task)));
    }

    /** Lets the engine close a task observation where it's done with it, and again at the end of the batch. */
    private static final class ClosedOnce implements TaskObserver.TaskObservation {

        private final TaskObserver.TaskObservation observation;
        private final AtomicBoolean closed = new AtomicBoolean();

        ClosedOnce(TaskObserver.TaskObservation observation) {
            this.observation = observation;
        }

        @Override
        public TaskObserver.Scope open() {
            return observation.open();
        }

        @Override
        public void error(Throwable error) {
            observation.error(error);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                observation.close();
            }
        }
    }

    /** Returns the final decisions for the runnable tasks, or null if the batch was cancelled by a stop. */
    private @Nullable Decisions process(Topic<T> topic, List<Task<T>> runnable, Map<BatchResource<?>, Object> opened, TaskObserver.BatchObservation observation) {
        List<Task<?>> tasks = List.copyOf(runnable);
        try (BatchScope scope = scope(topic)) {
            for (BatchResource<?> resource : topic.resources()) {
                opened.put(resource, resource.open(topic.ref(), tasks));
            }
            DefaultBatch<T> batch = new DefaultBatch<>(topic.ref(), runnable, opened, core.tasks()::checkTarget, this::observe);
            Decisions handled = covering(scope.call(() -> {
                try (TaskObserver.Scope ignored = observation.open()) {
                    return HandlerThread.run(topic.ref().name(), () -> topic.handler().handle(batch));
                }
            }, topic.settings().batchDeadline()), runnable);
            Decisions merged = handled;
            for (Map.Entry<BatchResource<?>, Object> resource : opened.entrySet()) {
                merged = merged.merge(review(resource.getKey(), resource.getValue(), handled));
            }
            return merged;
        } catch (TimeoutException e) {
            observation.error(e);
            return Decisions.all(runnable, Decision.failed("Batch deadline of " + topic.settings().batchDeadline() + " exceeded"));
        } catch (InterruptedException | CancellationException e) {
            if (worker.stopping()) {
                Thread.interrupted();
                return null;
            }
            observation.error(e);
            return Decisions.all(runnable, Decision.failed(e));
        } catch (Exception e) {
            observation.error(e);
            return Decisions.all(runnable, Decision.failed(e));
        } catch (Error e) {
            // Without this, an Error left the claims to the orphan sweep, which hands them back
            // without counting the attempt: retried forever, no error stored.
            if (e instanceof VirtualMachineError vm) {
                fatal = vm;
            }
            observation.error(e);
            return Decisions.all(runnable, Decision.failed(e));
        }
    }

    private BatchScope scope(Topic<T> topic) {
        scope = new BatchScope("pgtasks-" + topic.ref().name() + "-batch", worker::stopping);
        return scope;
    }

    /** Fails the tasks the handler left out, and rejects decisions for tasks it wasn't given. */
    private static <T> Decisions covering(Decisions handled, List<Task<T>> runnable) {
        if (handled == null) {
            throw new IllegalStateException("The handler returned no decisions");
        }
        Set<Long> expected = new LinkedHashSet<>();
        runnable.forEach(task -> expected.add(task.id()));
        for (Task<?> task : handled.tasks()) {
            if (!expected.contains(task.id())) {
                throw new IllegalStateException("The handler decided on task " + task.id() + ", which is not in the batch");
            }
        }
        if (handled.tasks().size() == runnable.size()) {
            return handled;
        }
        Decisions.Builder complete = Decisions.builder();
        // With their junctions: without them, a fan-in group a decided task belongs to stays suspended for good.
        handled.verdicts().forEach(verdict -> complete.decide(verdict.task(), verdict.decision(), verdict.downstream(), verdict.junctions()));
        runnable.stream().filter(task -> !handled.covers(task)).forEach(task -> complete.decide(task, Decision.failed("The handler returned no decision for this task")));
        return complete.build();
    }

    @SuppressWarnings("unchecked")
    private static <R> Decisions review(BatchResource<R> resource, Object value, Decisions decisions) throws Exception {
        return resource.review((R) value, decisions);
    }

    @SuppressWarnings("unchecked")
    private static <R> void beforeCommit(BatchResource<R> resource, Object value, Decisions decisions, Connection connection) throws SQLException {
        resource.beforeCommit((R) value, decisions, connection);
    }

    @SuppressWarnings("unchecked")
    private static <R> void afterCommit(BatchResource<R> resource, Object value, Decisions decisions) {
        resource.afterCommit((R) value, decisions);
    }

    private void complete(Topic<T> topic, Map<Long, ClaimedTask> claims, Decisions decisions, Decisions early, Map<BatchResource<?>, Object> opened,
                          TaskObserver.BatchObservation observation) {
        // The node-wide cap on talking to the store, taken again for the completion.
        core.inFlight().acquireUninterruptibly();
        try {
            completeWithin(topic, claims, decisions, early, opened, observation);
        } finally {
            core.inFlight().release();
        }
    }

    private void completeWithin(Topic<T> topic, Map<Long, ClaimedTask> claims, Decisions decisions, Decisions early, Map<BatchResource<?>, Object> opened,
                                TaskObserver.BatchObservation observation) {
        List<Completion> completions = new ArrayList<>(claims.size());
        decisions.verdicts().forEach(verdict -> completions.add(completion(claimOf(claims, verdict.task()), verdict.decision())));
        early.verdicts().forEach(verdict -> completions.add(completion(claimOf(claims, verdict.task()), verdict.decision())));
        Set<String> targets = new LinkedHashSet<>();
        Set<Integer> ready = new LinkedHashSet<>();
        List<Runnable> observations = new ArrayList<>();
        try {
            core.transactRetrying("The completion of a batch in " + topic.ref(), transaction -> {
                observations.clear();
                targets.clear();
                ready.clear();
                core.store().complete(transaction, worker.topicId(), core.nodeId(), completions);
                targets.addAll(pushDownstream(transaction, decisions, observations, ready));
                targets.addAll(applyJunctions(transaction, decisions));
                for (Map.Entry<BatchResource<?>, Object> resource : opened.entrySet()) {
                    beforeCommit(resource.getKey(), resource.getValue(), decisions, transaction.connection());
                }
                return null;
            });
        } catch (StaleClaimException stale) {
            LOGGER.warn("Discarded the completion of a batch in {}: {}", topic.ref(), stale.getMessage());
            release(claims, claims.keySet().stream().filter(id -> !stale.lost().contains(id)).toList());
            observation.released();
            return;
        } catch (Exception | Error e) {
            if (e instanceof VirtualMachineError vm) {
                fatal = vm;
            }
            if (Transient.is(e) && core.stopping().getAsBoolean()) {
                // The completion gave up on a transient error (a deadlock, the database restarting)
                // only because this node is stopping. Nothing is wrong with the tasks: hand them
                // back without an attempt, as stop does with a batch it cuts short.
                LOGGER.warn("The completion of a batch in {} was cut short by stop on a transient error; its tasks are handed back: {}",
                    topic.ref(), e.toString());
                release(claims, List.copyOf(claims.keySet()));
                observation.released();
                return;
            }
            LOGGER.warn("The completion of a batch in {} failed and was rolled back", topic.ref(), e);
            observation.error(e);
            List<Completion> failures = claims.values().stream().map(claim -> completion(claim, Decision.failed(e))).toList();
            try {
                core.transactRetrying("Recording a failed batch in " + topic.ref(), transaction -> core.store().release(transaction, worker.topicId(), core.nodeId(), failures));
                observation.completed(results(failures));
                tell(topic, failures, claims);
            } catch (Exception recording) {
                LOGGER.error("Could not record the failure; the tasks stay active until this node's claims expire", recording);
            }
            return;
        }
        observation.completed(results(completions));
        tell(topic, completions, claims);
        observations.forEach(Runnable::run);
        for (Map.Entry<BatchResource<?>, Object> resource : opened.entrySet()) {
            try {
                afterCommit(resource.getKey(), resource.getValue(), decisions);
            } catch (RuntimeException e) {
                LOGGER.warn("afterCommit of {} failed after the completion committed", resource.getKey(), e);
            }
        }
        targets.forEach(core.wake());
        core.tasks().committed(ready);
    }

    /**
     * Pushes the downstream work of continuing tasks, one statement per target topic and insertion.
     * Each non-APPEND push locks its identifiers until commit, so the targets go in topic-name
     * order: two completions pushing to the same topics then lock them in the same order and
     * can't deadlock on them. Within a topic, targets keep the order the handlers pushed in, since
     * a later insertion may act on an earlier one's tasks; two completions that push one topic
     * with different insertions in opposite orders can still deadlock, and the completion retries.
     */
    private Set<String> pushDownstream(StoreTransaction transaction, Decisions decisions, List<Runnable> observations, Set<Integer> ready) {
        record Target(TopicRef<?> topic, Insertion insertion) {
        }
        Map<Target, List<Push<?>>> pushes = new LinkedHashMap<>();
        Map<Target, List<Task<?>>> parents = new LinkedHashMap<>();
        for (Decisions.Verdict verdict : decisions.verdicts()) {
            if (!verdict.decision().continues()) {
                continue;
            }
            for (Push<?> push : verdict.downstream()) {
                Target target = new Target(push.topic(), push.insertion());
                pushes.computeIfAbsent(target, ignored -> new ArrayList<>()).add(push);
                parents.computeIfAbsent(target, ignored -> new ArrayList<>()).add(verdict.task());
            }
        }
        Set<String> targets = new LinkedHashSet<>();
        for (Map.Entry<Target, List<Push<?>>> entry : byTopicName(pushes, target -> target.topic().name())) {
            observations.add(write(transaction, entry.getKey().topic(), entry.getKey().insertion(), entry.getValue(), Objects.requireNonNull(parents.get(entry.getKey())), ready));
            targets.add(entry.getKey().topic().name());
        }
        return targets;
    }

    @SuppressWarnings("unchecked")
    private <C> Runnable write(StoreTransaction transaction, TopicRef<C> topic, Insertion insertion, List<Push<?>> pushes, List<Task<?>> parents,
                               Set<Integer> ready) {
        List<Creation<C>> creations = pushes.stream().map(push -> (Creation<C>) push.creation()).toList();
        List<Pushed> results = core.tasks().write(transaction, topic, insertion, creations, parents, parent -> observe(parent).open(), ready);
        return () -> core.tasks().observed(topic, insertion, creations, results);
    }

    /**
     * Applies the junctions of continuing tasks, after their downstream pushes, so a suspended
     * task pushed in the same completion can complete a group. One call per target and counts,
     * in topic-name order like the pushes, since each call locks its groups' members until commit.
     */
    private Set<String> applyJunctions(StoreTransaction transaction, Decisions decisions) throws SQLException {
        record Target(String topic, Junction.Counts counts) {
        }
        Map<Target, List<Set<String>>> groups = new LinkedHashMap<>();
        for (Decisions.Verdict verdict : decisions.verdicts()) {
            if (!verdict.decision().continues()) {
                continue;
            }
            for (Junction junction : verdict.junctions()) {
                Set<String> group = new LinkedHashSet<>(junction.identifiers());
                group.add(verdict.task().identifier());
                groups.computeIfAbsent(new Target(junction.topic().name(), junction.counts()), ignored -> new ArrayList<>()).add(group);
            }
        }
        Set<String> released = new LinkedHashSet<>();
        for (Map.Entry<Target, List<Set<String>>> entry : byTopicName(groups, Target::topic)) {
            int topicId = core.tasks().topicId(transaction, entry.getKey().topic());
            try {
                if (!core.store().junction(transaction, topicId, entry.getKey().counts(), entry.getValue()).isEmpty()) {
                    released.add(entry.getKey().topic());
                }
            } catch (SQLException e) {
                core.tasks().failed(entry.getKey().topic(), topicId, e);
                throw e;
            }
        }
        return released;
    }

    /** The entries sorted by topic name; a stable sort, so entries for one topic keep their order. */
    private static <K, V> List<Map.Entry<K, V>> byTopicName(Map<K, V> map, Function<K, String> topic) {
        return map.entrySet().stream().sorted(Comparator.comparing(entry -> topic.apply(entry.getKey()))).toList();
    }

    /** The claim of a task in this batch. */
    private static ClaimedTask claimOf(Map<Long, ClaimedTask> claims, Task<?> task) {
        return claimOf(claims, task.id());
    }

    private static ClaimedTask claimOf(Map<Long, ClaimedTask> claims, long taskId) {
        return Objects.requireNonNull(claims.get(taskId), () -> "Task " + taskId + " is not in this batch");
    }

    private void release(Map<Long, ClaimedTask> claims, List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        List<Completion> releases = ids.stream().map(id -> {
            ClaimedTask claim = Objects.requireNonNull(claims.get(id));
            return new Completion(claim.id(), claim.attempt(), claim.claimedAt(), RELEASE);
        }).toList();
        try {
            core.transactRetrying("Releasing tasks in " + worker.topic().ref(), transaction -> core.store().release(transaction, worker.topicId(), core.nodeId(), releases));
        } catch (Exception e) {
            LOGGER.warn("Could not release tasks {}; they stay active until this node's claims expire", ids, e);
        }
    }

    private Completion completion(ClaimedTask claim, Decision decision) {
        Retry retry = worker.topic().settings().retry();
        Resolution resolution = switch (decision) {
            case Decision.Success success -> new Resolution.Finished(Outcome.SUCCEEDED, success.output());
            case Decision.Filtered filtered -> new Resolution.Finished(Outcome.FILTERED, filtered.reason());
            case Decision.Suspended ignored -> new Resolution.Waiting(WaitState.SUSPENDED, null, false, null);
            case Decision.Later later -> new Resolution.Waiting(WaitState.SCHEDULED, later.delay(), false, null);
            case Decision.Failed failed -> {
                String error = ErrorJson.of(failed.message(), failed.cause());
                // The backoff follows the failures in a row, counted or not, so it keeps growing through an outage.
                yield switch (retry.classify(failed.cause())) {
                    case NOT_COUNTED -> new Resolution.Waiting(WaitState.RETRYING, retry.uncountedBackoff(claim.failures() + 1, core.random()), false, error);
                    case DEAD_LETTER -> new Resolution.Waiting(WaitState.DEAD_LETTER, null, true, error);
                    case COUNTED -> {
                        Duration backoff = retry.backoff(claim.failures() + 1, core.random());
                        yield retry.allowsAnotherAfter(claim.attempt(), claim.firstFailedAt(), core.clock().instant(), backoff)
                            ? new Resolution.Waiting(WaitState.RETRYING, backoff, true, error)
                            : new Resolution.Waiting(WaitState.DEAD_LETTER, null, true, error);
                    }
                };
            }
        };
        return new Completion(claim.id(), claim.attempt(), claim.claimedAt(), resolution);
    }

    /** How many of the completions ended in each result. */
    private static Map<TaskObserver.Result, Integer> results(List<Completion> completions) {
        Map<TaskObserver.Result, Integer> results = new EnumMap<>(TaskObserver.Result.class);
        for (Completion completion : completions) {
            results.merge(result(completion), 1, Integer::sum);
        }
        return results;
    }

    private static TaskObserver.Result result(Completion completion) {
        return switch (completion.resolution()) {
            case Resolution.Finished finished -> finished.outcome() == Outcome.SUCCEEDED ? TaskObserver.Result.SUCCEEDED : TaskObserver.Result.FILTERED;
            case Resolution.Waiting waiting -> switch (waiting.state()) {
                case SUSPENDED -> TaskObserver.Result.SUSPENDED;
                case SCHEDULED -> TaskObserver.Result.DELAYED;
                case RETRYING -> waiting.counted() || waiting.error() == null ? TaskObserver.Result.RETRYING : TaskObserver.Result.UNAVAILABLE;
                case DEAD_LETTER -> TaskObserver.Result.DEAD_LETTERED;
            };
        };
    }

    /** Tells the extensions' batch listeners what became of each task. */
    private void tell(Topic<T> topic, List<Completion> completions, Map<Long, ClaimedTask> claims) {
        if (core.listeners().isEmpty()) {
            return;
        }
        List<BatchListener.Completed> results = completions.stream()
            .map(completion -> new BatchListener.Completed(completion.taskId(), claimOf(claims, completion.taskId()).identifier(),
                completion.attempt(), result(completion)))
            .toList();
        for (BatchListener listener : core.listeners()) {
            try {
                listener.completed(topic.ref(), results);
            } catch (RuntimeException e) {
                LOGGER.warn("A batch listener failed for {}", topic.ref(), e);
            }
        }
    }

    private static void closeQuietly(Object value) {
        if (value instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOGGER.warn("Could not close a batch resource", e);
            }
        }
    }
}
