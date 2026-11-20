package org.example.task2.engine;

import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import org.example.task2.BatchResource;
import org.example.task2.Creation;
import org.example.task2.Decision;
import org.example.task2.Decisions;
import org.example.task2.Insertion;
import org.example.task2.Junction;
import org.example.task2.Juncture;
import org.example.task2.Push;
import org.example.task2.Task;
import org.example.task2.TopicRef;
import org.example.task2.spi.StaleClaimException;
import org.example.task2.spi.StoreTransaction;
import org.example.task2.spi.TaskStore.ClaimedTask;
import org.example.task2.spi.TaskStore.Completion;
import org.example.task2.spi.TaskStore.Outcome;
import org.example.task2.spi.TaskStore.Resolution;
import org.example.task2.spi.TaskStore.WaitState;

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

    private static final System.Logger LOGGER = System.getLogger(BatchRun.class.getName());

    private static final Resolution RELEASE = new Resolution.Waiting(WaitState.RETRYING, Duration.ZERO, false, null);

    private final TopicWorker<T> worker;
    private final EngineCore core;
    private final List<ClaimedTask> claimed;

    BatchRun(TopicWorker<T> worker, EngineCore core, List<ClaimedTask> claimed) {
        this.worker = worker;
        this.core = core;
        this.claimed = claimed;
    }

    void run() {
        Topic<T> topic = worker.topic();
        Map<Long, ClaimedTask> claims = new LinkedHashMap<>();
        List<Task<T>> runnable = new ArrayList<>();
        Decisions.Builder early = Decisions.builder();
        for (ClaimedTask claim : claimed) {
            claims.put(claim.id(), claim);
            T payload;
            try {
                payload = claim.payload() == null ? null : topic.codec().decode(claim.payload());
            } catch (RuntimeException e) {
                early.decide(task(claim, null), Decision.failed(new IllegalStateException("Could not decode the payload", e)));
                continue;
            }
            Task<T> task = task(claim, payload);
            if (claim.attempt() > topic.settings().retry().maxAttempts()) {
                early.decide(task, Decision.failed("No attempts left; the last one expired"));
            } else {
                runnable.add(task);
            }
        }
        Map<BatchResource<?>, Object> opened = new LinkedHashMap<>();
        try {
            Decisions decisions = runnable.isEmpty() ? Decisions.builder().build() : process(topic, runnable, opened);
            if (decisions == null) {
                release(claims, claimed.stream().map(ClaimedTask::id).toList());
                return;
            }
            complete(topic, claims, decisions, early.build(), opened);
        } finally {
            opened.values().forEach(BatchRun::closeQuietly);
        }
    }

    /** Hands every claimed task back without running it or counting the attempt. */
    void releaseAll() {
        Map<Long, ClaimedTask> claims = new LinkedHashMap<>();
        claimed.forEach(claim -> claims.put(claim.id(), claim));
        release(claims, List.copyOf(claims.keySet()));
    }

    private Task<T> task(ClaimedTask claim, T payload) {
        return new Task<>(worker.topic().ref(), claim.id(), claim.identifier(), payload, claim.attempt(), claim.reference(),
            claim.parentId(), claim.rootId(), claim.createdAt(), claim.claimedAt());
    }

    /** Returns the final decisions for the runnable tasks, or null if the batch was cancelled by a stop. */
    private Decisions process(Topic<T> topic, List<Task<T>> runnable, Map<BatchResource<?>, Object> opened) {
        List<Task<?>> tasks = List.copyOf(runnable);
        try (BatchScope scope = new BatchScope("task2-" + topic.ref().name() + "-batch")) {
            for (BatchResource<?> resource : topic.resources()) {
                opened.put(resource, resource.open(topic.ref(), tasks));
            }
            DefaultBatch<T> batch = new DefaultBatch<>(topic.ref(), runnable, opened);
            Decisions handled = covering(scope.call(() -> topic.handler().handle(batch), topic.settings().batchDeadline()), runnable);
            Decisions merged = handled;
            for (Map.Entry<BatchResource<?>, Object> resource : opened.entrySet()) {
                merged = merged.merge(review(resource.getKey(), resource.getValue(), handled));
            }
            return merged;
        } catch (TimeoutException e) {
            return Decisions.all(runnable, Decision.failed("Batch deadline of " + topic.settings().batchDeadline() + " exceeded"));
        } catch (InterruptedException | CancellationException e) {
            if (worker.stopping()) {
                Thread.interrupted();
                return null;
            }
            return Decisions.all(runnable, Decision.failed(e));
        } catch (Exception e) {
            return Decisions.all(runnable, Decision.failed(e));
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
    private static <R> void beforeCommit(BatchResource<R> resource, Object value, Decisions decisions, Connection connection) throws java.sql.SQLException {
        resource.beforeCommit((R) value, decisions, connection);
    }

    @SuppressWarnings("unchecked")
    private static <R> void afterCommit(BatchResource<R> resource, Object value, Decisions decisions) {
        resource.afterCommit((R) value, decisions);
    }

    private void complete(Topic<T> topic, Map<Long, ClaimedTask> claims, Decisions decisions, Decisions early, Map<BatchResource<?>, Object> opened) {
        List<Completion> completions = new ArrayList<>(claims.size());
        decisions.verdicts().forEach(verdict -> completions.add(completion(claims.get(verdict.task().id()), verdict.decision())));
        early.verdicts().forEach(verdict -> completions.add(completion(claims.get(verdict.task().id()), verdict.decision())));
        Set<String> targets = new LinkedHashSet<>();
        List<Runnable> observations = new ArrayList<>();
        try {
            core.transact(transaction -> {
                core.store().complete(transaction, worker.topicId(), core.nodeId(), completions);
                targets.addAll(pushDownstream(transaction, decisions, observations));
                targets.addAll(applyJunctions(transaction, decisions));
                for (Map.Entry<BatchResource<?>, Object> resource : opened.entrySet()) {
                    beforeCommit(resource.getKey(), resource.getValue(), decisions, transaction.connection());
                }
                return null;
            });
        } catch (StaleClaimException stale) {
            LOGGER.log(System.Logger.Level.WARNING, "Discarded the completion of a batch in " + topic.ref() + ": " + stale.getMessage());
            release(claims, claims.keySet().stream().filter(id -> !stale.lost().contains(id)).toList());
            return;
        } catch (Exception e) {
            LOGGER.log(System.Logger.Level.WARNING, "The completion of a batch in " + topic.ref() + " failed and was rolled back", e);
            List<Completion> failures = claims.values().stream().map(claim -> completion(claim, Decision.failed(e))).toList();
            try {
                core.transact(transaction -> core.store().release(transaction, worker.topicId(), core.nodeId(), failures));
            } catch (Exception recording) {
                LOGGER.log(System.Logger.Level.ERROR, "Could not record the failure; the tasks stay active until this node's claims expire", recording);
            }
            return;
        }
        observations.forEach(Runnable::run);
        for (Map.Entry<BatchResource<?>, Object> resource : opened.entrySet()) {
            try {
                afterCommit(resource.getKey(), resource.getValue(), decisions);
            } catch (RuntimeException e) {
                LOGGER.log(System.Logger.Level.WARNING, "afterCommit of " + resource.getKey() + " failed after the completion committed", e);
            }
        }
        targets.forEach(core.wake());
    }

    /** Pushes the downstream work of continuing tasks, one statement per target topic and insertion. */
    private Set<String> pushDownstream(StoreTransaction transaction, Decisions decisions, List<Runnable> observations) {
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
        for (Map.Entry<Target, List<Push<?>>> entry : pushes.entrySet()) {
            observations.add(write(transaction, entry.getKey().topic(), entry.getKey().insertion(), entry.getValue(), parents.get(entry.getKey())));
            targets.add(entry.getKey().topic().name());
        }
        return targets;
    }

    @SuppressWarnings("unchecked")
    private <C> Runnable write(StoreTransaction transaction, TopicRef<C> topic, Insertion insertion, List<Push<?>> pushes, List<Task<?>> parents) {
        List<Creation<C>> creations = pushes.stream().map(push -> (Creation<C>) push.creation()).toList();
        List<org.example.task2.Pushed> results = core.tasks().write(transaction, topic, insertion, creations, parents);
        return () -> core.tasks().observed(topic, insertion, creations, results);
    }

    /**
     * Applies the junctions of continuing tasks, after their downstream pushes, so a suspended
     * task pushed in the same completion can complete a group. One call per target and juncture.
     */
    private Set<String> applyJunctions(StoreTransaction transaction, Decisions decisions) throws java.sql.SQLException {
        record Target(String topic, Juncture juncture) {
        }
        Map<Target, List<Set<String>>> groups = new LinkedHashMap<>();
        for (Decisions.Verdict verdict : decisions.verdicts()) {
            if (!verdict.decision().continues()) {
                continue;
            }
            for (Junction junction : verdict.junctions()) {
                Set<String> group = new LinkedHashSet<>(junction.identifiers());
                group.add(verdict.task().identifier());
                groups.computeIfAbsent(new Target(junction.topic().name(), junction.juncture()), ignored -> new ArrayList<>()).add(group);
            }
        }
        Set<String> released = new LinkedHashSet<>();
        for (Map.Entry<Target, List<Set<String>>> entry : groups.entrySet()) {
            int topicId = core.tasks().topicId(transaction, entry.getKey().topic());
            if (!core.store().junction(transaction, topicId, entry.getKey().juncture(), entry.getValue()).isEmpty()) {
                released.add(entry.getKey().topic());
            }
        }
        return released;
    }

    private void release(Map<Long, ClaimedTask> claims, List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        List<Completion> releases = ids.stream().map(id -> {
            ClaimedTask claim = claims.get(id);
            return new Completion(claim.id(), claim.attempt(), claim.claimedAt(), RELEASE);
        }).toList();
        try {
            core.transact(transaction -> core.store().release(transaction, worker.topicId(), core.nodeId(), releases));
        } catch (Exception e) {
            LOGGER.log(System.Logger.Level.WARNING, "Could not release tasks " + ids + "; they stay active until this node's claims expire", e);
        }
    }

    private Completion completion(ClaimedTask claim, Decision decision) {
        Retry retry = worker.topic().settings().retry();
        Resolution resolution = switch (decision) {
            case Decision.Success ignored -> new Resolution.Finished(Outcome.SUCCEEDED, null);
            case Decision.Filtered filtered -> new Resolution.Finished(Outcome.FILTERED, filtered.reason());
            case Decision.Suspended ignored -> new Resolution.Waiting(WaitState.SUSPENDED, null, false, null);
            case Decision.Later later -> new Resolution.Waiting(WaitState.SCHEDULED, later.delay(), false, null);
            case Decision.Throttled throttled -> new Resolution.Waiting(WaitState.RETRYING, throttled.retryAfter(), false, null);
            case Decision.Failed failed -> {
                String error = ErrorJson.of(failed.message(), failed.cause());
                yield retry.allowsAnotherAfter(claim.attempt())
                    ? new Resolution.Waiting(WaitState.RETRYING, retry.backoff(claim.attempt(), core.random()), true, error)
                    : new Resolution.Waiting(WaitState.DEAD_LETTER, null, true, error);
            }
        };
        return new Completion(claim.id(), claim.attempt(), claim.claimedAt(), resolution);
    }

    private static void closeQuietly(Object value) {
        if (value instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOGGER.log(System.Logger.Level.WARNING, "Could not close a batch resource", e);
            }
        }
    }
}
