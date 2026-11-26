package io.github.khopland.pgtasks.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.IdentityHashMap;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.github.khopland.pgtasks.BatchResource;
import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Decisions;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Push;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TaskStorageException;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.extension.BatchListener;
import io.github.khopland.pgtasks.spi.StaleClaimException;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore.ClaimedTask;
import io.github.khopland.pgtasks.spi.TaskStore.Completion;
import io.github.khopland.pgtasks.spi.TaskStore.Outcome;
import io.github.khopland.pgtasks.spi.TaskStore.Resolution;
import io.github.khopland.pgtasks.spi.TaskStore.WaitState;
import io.github.khopland.pgtasks.spi.TaskStore;

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
    private volatile @Nullable BatchScope scope;
    /** Set when the node fenced itself (see {@link HeartbeatFence}): the batch is handed back unless its handler already decided. */
    private volatile boolean fenced;
    /** A VirtualMachineError from the handler: recorded as the batch's failure, then rethrown. */
    private @Nullable VirtualMachineError fatal;
    /** Tasks whose decider never started before the deadline: handed back with the completion, without an attempt. */
    private List<Long> unstarted = List.of();
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
        claimed.forEach(claim -> claims.put(claim.id(), claim));
        List<Task<T>> runnable = new ArrayList<>();
        List<Task<?>> all = new ArrayList<>(claimed.size());
        Decisions.Builder early = Decisions.builder();
        try {
            prepare(topic, runnable, all, early);
        } catch (RuntimeException e) {
            // Before anything ran: without this the claims stayed held here, out of the orphan
            // sweep's reach, until the node restarted.
            failUnexpectedly(topic, claims, e);
            claimed.forEach(claim -> core.held().remove(claim.id(), claim.attempt()));
            return;
        }
        runPrepared(topic, claims, runnable, all, early, new LinkedHashMap<>());
    }

    /**
     * Makes each claim a task: decoded, or failed at once if its payload can't be, or if it has
     * no attempts left. Its headers are only what a task can hold, as an observer's are.
     */
    private void prepare(Topic<T> topic, List<Task<T>> runnable, List<Task<?>> all, Decisions.Builder early) {
        for (ClaimedTask claim : claimed) {
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
    }

    private void runPrepared(Topic<T> topic, Map<Long, ClaimedTask> claims, List<Task<T>> runnable, List<Task<?>> all, Decisions.Builder early,
                             Map<BatchResource<?>, Object> opened) {
        TaskObserver.BatchObservation observation = core.observer().batch(topic.ref(), all);
        this.batchObservation = observation;
        try {
            runBatch(topic, claims, runnable, early.build(), opened, observation);
        } catch (RuntimeException e) {
            failUnexpectedly(topic, claims, e);
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
        IdentityHashMap<Creation<?>, @Nullable String> encoded = new IdentityHashMap<>();
        Decisions writable = writable(decisions, encoded);
        try (TaskObserver.Scope ignored = observation.open()) {
            complete(topic, claims, writable, early, opened, observation, encoded);
        }
    }

    /**
     * The decisions with every continuing task's downstream work checked before the completion:
     * its topic exists and has a codec, and its payloads encode to something the store can hold.
     * The completion writes it all in one transaction, so one task's push that can't be written
     * failed the whole batch: every task in it ran again, and the same task failed it again on
     * every retry. Such a task fails on its own now. A handler that stages with
     * {@code task.push(...)} is checked for its topic there already; one that builds its
     * {@link Decisions} itself is not, and payloads are only encoded here. The encoded payloads
     * are kept in {@code encoded}, so the completion doesn't encode them again.
     */
    private Decisions writable(Decisions decisions, Map<Creation<?>, @Nullable String> encoded) {
        Decisions checked = decisions;
        for (Decisions.Verdict verdict : decisions.verdicts()) {
            if (!verdict.decision().continues()) {
                continue;
            }
            try {
                for (Push<?> push : verdict.downstream()) {
                    encode(push, encoded);
                }
            } catch (TaskStorageException e) {
                // Looking the topic up failed, not the push: the completion tries again.
            } catch (RuntimeException | LinkageError | StackOverflowError e) {
                observe(verdict.task()).error(e);
                checked = checked.revise(verdict.task(), Decision.failed(e));
            }
        }
        return checked;
    }

    private <C> void encode(Push<C> push, Map<Creation<?>, @Nullable String> encoded) {
        core.tasks().checkTarget(push.topic());
        encoded.put(push.creation(), core.tasks().encode(push.topic(), push.creation()));
    }

    /** How long a task waits after a failure the engine couldn't put through the retry policy. */
    static final Duration UNEXPECTED_BACKOFF = Duration.ofMinutes(1);

    /**
     * Records a failure that escaped the batch's own handling, a bug in the engine or in what
     * plugs into it, as a counted failure of every task the batch still holds. Left alone, the
     * claims would go to the orphan sweep, which hands them back without counting the attempt or
     * keeping the error: the task would run again and again, and nothing would show why. Counted,
     * a task that keeps failing this way is dead-lettered once its attempts are used up, as a
     * claim past the policy's attempts is when it is next claimed. Fenced like any completion, so
     * tasks whose completion already committed are left as they are.
     */
    private void failUnexpectedly(Topic<T> topic, Map<Long, ClaimedTask> claims, RuntimeException e) {
        LOGGER.error("A batch in {} failed unexpectedly; its tasks are recorded as failed", topic.ref(), e);
        if (scope != null && scope.abandoned()) {
            return;
        }
        String error = ErrorJson.of("The batch failed unexpectedly", e);
        List<Completion> failures = claims.values().stream()
            .map(claim -> new Completion(claim.id(), claim.attempt(), claim.claimedAt(),
                new Resolution.Waiting(WaitState.RETRYING, UNEXPECTED_BACKOFF, true, error)))
            .toList();
        try {
            core.transactRetrying("Recording a failed batch in " + topic.ref(), transaction -> core.store().release(transaction, worker.topicId(), core.nodeId(), failures));
        } catch (Exception recording) {
            LOGGER.error("Could not record the failure; the tasks stay active until this node's claims expire", recording);
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
            claim.parentId(), claim.rootId(), claim.createdAt(), claim.claimedAt(), SafeObserver.storable(claim.headers()));
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

    /**
     * Cancels the handler because the node's heartbeat is too late: its tasks go back without
     * counting the attempt, as on a stop. A handler that already returned keeps its decisions,
     * since its completion is fenced in the store anyway.
     */
    void fence() {
        fenced = true;
        BatchScope current = scope;
        if (current != null) {
            current.cancel();
        }
    }

    /** Returns the final decisions for the runnable tasks, or null if the batch was cancelled by a stop or a fence. */
    private @Nullable Decisions process(Topic<T> topic, List<Task<T>> runnable, Map<BatchResource<?>, Object> opened, TaskObserver.BatchObservation observation) {
        if (fenced || core.fenced().getAsBoolean()) {
            return null;
        }
        List<Task<?>> tasks = List.copyOf(runnable);
        try (BatchScope scope = scope(topic)) {
            for (BatchResource<?> resource : topic.resources()) {
                opened.put(resource, resource.open(topic.ref(), tasks));
            }
            DefaultBatch<T> batch = new DefaultBatch<>(topic.ref(), runnable, opened, core.tasks()::checkTarget, this::observe);
            AtomicReference<DefaultBatch.@Nullable Progress> atDeadline = new AtomicReference<>();
            Decisions handled;
            try {
                handled = covering(scope.call(() -> {
                    try (TaskObserver.Scope ignored = observation.open()) {
                        return HandlerThread.run(topic.ref().name(), () -> topic.handler().handle(batch));
                    }
                }, topic.settings().batchDeadline(), () -> atDeadline.set(batch.seal())), runnable);
            } catch (TimeoutException e) {
                if (fenced) {
                    return null;
                }
                observation.error(e);
                handled = pastDeadline(topic, runnable, Objects.requireNonNull(atDeadline.get()));
            }
            Decisions merged = handled;
            for (Map.Entry<BatchResource<?>, Object> resource : opened.entrySet()) {
                merged = merged.merge(review(resource.getKey(), resource.getValue(), handled));
            }
            return merged;
        } catch (InterruptedException | CancellationException e) {
            if (worker.stopping() || fenced) {
                Thread.interrupted();
                return null;
            }
            observation.error(e);
            return Decisions.all(runnable, Decision.failed(e));
        } catch (Exception e) {
            if (fenced) {
                // Whatever the cancellation made the handler throw, or the scope refusing work
                // cancelled before it started.
                Thread.interrupted();
                return null;
            }
            observation.error(e);
            return Decisions.all(runnable, Decision.failed(e));
        } catch (Error e) {
            // Without this, an Error left the claims to the orphan sweep, which hands them back
            // without counting the attempt: retried forever, no error stored.
            if (e instanceof VirtualMachineError vm) {
                fatal = vm;
            }
            if (fenced) {
                Thread.interrupted();
                return null;
            }
            observation.error(e);
            return Decisions.all(runnable, Decision.failed(e));
        }
    }

    /**
     * The decisions of a batch whose handler ran out of time. Before, every task failed, its
     * attempt counted: with {@code decideConcurrently}, one remote call that hung failed the tasks
     * already done, which then ran again. Now a task whose decider returned before the deadline
     * keeps its decision, and one whose decider was still running fails. A task whose decider
     * never started didn't run, so it goes back without counting an attempt (see
     * {@link #unstarted}), unless no decider started at all: then the handler itself ran out of time
     * before deciding, and every task fails as before, so a handler that always does can't hand
     * its tasks back forever.
     */
    private Decisions pastDeadline(Topic<T> topic, List<Task<T>> runnable, DefaultBatch.Progress progress) {
        Decision exceeded = Decision.failed("Batch deadline of " + topic.settings().batchDeadline() + " exceeded");
        if (progress.started().isEmpty()) {
            return Decisions.all(runnable, exceeded);
        }
        Decisions.Builder decisions = Decisions.builder();
        List<Long> handedBack = new ArrayList<>();
        for (Task<T> task : runnable) {
            Decisions.Verdict verdict = progress.decided().get(task.id());
            if (verdict != null) {
                decisions.decide(task, verdict.decision(), verdict.downstream());
            } else if (progress.started().contains(task.id())) {
                decisions.decide(task, exceeded);
            } else {
                handedBack.add(task.id());
            }
        }
        unstarted = List.copyOf(handedBack);
        return decisions.build();
    }

    private BatchScope scope(Topic<T> topic) {
        BatchScope created = new BatchScope("pgtasks-" + topic.ref().name() + "-batch", worker::stopping, this::noteIgnoredCancellation,
            core.threads().get());
        scope = created;
        if (fenced) {
            // Fenced while the scope was made: fence() may not have seen it.
            created.cancel();
        }
        return created;
    }

    /**
     * Leaves an error on the batch's tasks while their handler ignores cancellation, so search and
     * the dashboard show why they stay active. Their claims stay, so no other run can start beside
     * the one still going; the error is replaced by the batch's own once the handler ends.
     */
    private void noteIgnoredCancellation() {
        String error = ErrorJson.typed("The handler ignored cancellation and still runs, since " + core.clock().instant()
            + ". The task stays active until it ends. Stopping this node frees it: other nodes requeue it once the node counts as dead.", "hung");
        List<TaskStore.ActiveClaim> claims = claimed.stream()
            .map(claim -> new TaskStore.ActiveClaim(worker.topicId(), claim.id(), claim.attempt(), claim.claimedAt()))
            .toList();
        try {
            core.transact(transaction -> core.store().noteError(transaction, worker.topicId(), core.nodeId(), claims, error));
        } catch (Exception e) {
            LOGGER.warn("Could not record on the tasks in {} that their handler ignored cancellation", worker.topic().ref(), e);
        }
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
        handled.verdicts().forEach(verdict -> complete.decide(verdict.task(), verdict.decision(), verdict.downstream()));
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
                          TaskObserver.BatchObservation observation, Map<Creation<?>, @Nullable String> encoded) {
        // The node-wide cap on talking to the store, taken again for the completion.
        core.inFlight().acquireUninterruptibly();
        try {
            completeWithin(topic, claims, decisions, early, opened, observation, encoded);
        } finally {
            core.inFlight().release();
        }
    }

    private void completeWithin(Topic<T> topic, Map<Long, ClaimedTask> claims, Decisions decisions, Decisions early, Map<BatchResource<?>, Object> opened,
                                TaskObserver.BatchObservation observation, Map<Creation<?>, @Nullable String> encoded) {
        List<Completion> completions = new ArrayList<>(claims.size());
        decisions.verdicts().forEach(verdict -> completions.add(completion(claimOf(claims, verdict.task()), verdict.decision())));
        early.verdicts().forEach(verdict -> completions.add(completion(claimOf(claims, verdict.task()), verdict.decision())));
        unstarted.forEach(id -> {
            ClaimedTask claim = claimOf(claims, id);
            completions.add(new Completion(claim.id(), claim.attempt(), claim.claimedAt(), RELEASE));
        });
        Set<String> targets = new LinkedHashSet<>();
        Set<Integer> ready = new LinkedHashSet<>();
        List<Runnable> observations = new ArrayList<>();
        try {
            core.transactRetrying("The completion of a batch in " + topic.ref(), transaction -> {
                observations.clear();
                targets.clear();
                ready.clear();
                core.store().complete(transaction, worker.topicId(), core.nodeId(), completions);
                targets.addAll(pushDownstream(transaction, decisions, observations, ready, encoded));
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
            if ((Transient.is(e) || Transient.mayPass(e)) && core.stopping().getAsBoolean()) {
                // The completion gave up on a transient error (a deadlock, the database restarting)
                // only because this node is stopping. Nothing is wrong with the tasks: hand them
                // back without an attempt, as stop does with a batch it cuts short.
                LOGGER.warn("The completion of a batch in {} was cut short by stop on a transient error; its tasks are handed back: {}",
                    topic.ref(), e.toString());
                release(claims, List.copyOf(claims.keySet()));
                observation.released();
                return;
            }
            if (fatal == null) {
                Decisions isolated = withoutUnwritableDownstream(decisions, encoded);
                if (isolated != decisions) {
                    // One task's downstream work could fail the whole batch, and every task in it
                    // was retried with that one until all of them were dead-lettered.
                    LOGGER.warn("The completion of a batch in {} failed; the tasks whose downstream work can't be written fail on their "
                        + "own, and the others complete again", topic.ref(), e);
                    completeWithin(topic, claims, isolated, early, opened, observation, encoded);
                    return;
                }
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
     * After the completion failed: the decisions with each continuing task whose downstream work
     * can't be written on its own failed instead, or the same decisions if every task's can, so
     * the failure lies elsewhere (the store, a resource). Each task's work is tried in a
     * transaction of its own that is always rolled back. A failure that is or may be transient
     * says nothing about the task, so it keeps its decision.
     */
    private Decisions withoutUnwritableDownstream(Decisions decisions, Map<Creation<?>, @Nullable String> encoded) {
        Decisions checked = decisions;
        for (Decisions.Verdict verdict : decisions.verdicts()) {
            if (!verdict.decision().continues() || verdict.downstream().isEmpty()) {
                continue;
            }
            Decisions alone = Decisions.builder().decide(verdict.task(), verdict.decision(), verdict.downstream()).build();
            try (StoreTransaction probe = core.store().begin()) {
                pushDownstream(probe, alone, new ArrayList<>(), new LinkedHashSet<>(), encoded);
                // Never committed: closing rolls it back.
            } catch (Exception | LinkageError | StackOverflowError e) {
                if (Transient.is(e) || Transient.mayPass(e)) {
                    continue;
                }
                observe(verdict.task()).error(e);
                checked = checked.revise(verdict.task(), Decision.failed(e));
            }
        }
        return checked;
    }

    /**
     * Pushes the downstream work of continuing tasks, one statement per target topic and insertion.
     * Each non-APPEND push locks its identifiers until commit, so the targets go in topic-name
     * order: two completions pushing to the same topics then lock them in the same order and
     * can't deadlock on them. Within a topic, targets keep the order the handlers pushed in, since
     * a later insertion may act on an earlier one's tasks; two completions that push one topic
     * with different insertions in opposite orders can still deadlock, and the completion retries.
     */
    private Set<String> pushDownstream(StoreTransaction transaction, Decisions decisions, List<Runnable> observations, Set<Integer> ready,
                                       Map<Creation<?>, @Nullable String> encoded) {
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
            observations.add(write(transaction, entry.getKey().topic(), entry.getKey().insertion(), entry.getValue(), Objects.requireNonNull(parents.get(entry.getKey())), ready,
                encoded));
            targets.add(entry.getKey().topic().name());
        }
        return targets;
    }

    @SuppressWarnings("unchecked")
    private <C> Runnable write(StoreTransaction transaction, TopicRef<C> topic, Insertion insertion, List<Push<?>> pushes, List<Task<?>> parents,
                               Set<Integer> ready, Map<Creation<?>, @Nullable String> encoded) {
        List<Creation<C>> creations = pushes.stream().map(push -> (Creation<C>) push.creation()).toList();
        List<Pushed> results = core.tasks().write(transaction, topic, insertion, creations, parents, parent -> observe(parent).open(), ready, encoded);
        return () -> core.tasks().observed(topic, insertion, creations, results);
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
