package org.example.task2.memory;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.example.task2.Insertion;
import org.example.task2.Juncture;
import org.example.task2.Ordering;
import org.example.task2.Pushed;
import org.example.task2.memory.Rows.State;
import org.example.task2.spi.Notifier;
import org.example.task2.spi.StaleClaimException;
import org.example.task2.spi.StoreTransaction;
import org.example.task2.spi.TaskStore;

/**
 * Tasks in memory, for running an app locally without a database and for fast tests. It passes
 * the same TCK as the Postgres store, so ordering, insertion policies, fencing, junctures,
 * retries and rate admission behave the same way. Several engines in one JVM can share one
 * instance; {@link #notifier()} gives each of them in-process wake-ups.
 *
 * <p>Limits: nothing survives a restart; wake-ups only reach engines in this JVM; and the store
 * can't share a transaction with the application's own tables, so pushes made in an application
 * transaction are applied after it commits. Atomicity with the application's data can only be
 * proven on Postgres. It is never chosen automatically.
 *
 * <p>Every transaction holds one store-wide lock from begin to commit or rollback, so
 * transactions on different threads never interleave. Time comes from the given clock; pass a
 * {@code ManualClock} to jump past backoffs, delays and expiry in tests.
 */
public final class InMemoryTaskStore implements TaskStore {

    private final InstantSource clock;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final List<Notifier.Listener> listeners = new CopyOnWriteArrayList<>();

    private final Map<Integer, Rows.Topic> topics = new LinkedHashMap<>();
    private final Map<Integer, Rows.Node> nodes = new HashMap<>();
    private final TreeMap<Long, Rows.Task> tasks = new TreeMap<>();
    private final TreeMap<Long, Rows.Queued> queue = new TreeMap<>();
    private final List<Rows.Attempt> attempts = new ArrayList<>();
    private final Map<String, Rows.Bucket> buckets = new HashMap<>();
    private int nextTopic = 1, nextNode = 1;
    private long nextTask = 1;

    public InMemoryTaskStore() {
        this(InstantSource.system());
    }

    public InMemoryTaskStore(InstantSource clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public InstantSource clock() {
        return clock;
    }

    /** In-process wake-ups for one engine; give every engine sharing this store its own. */
    public Notifier notifier() {
        return new Notifier() {
            private Listener listener;

            @Override
            public void start(Listener listener) {
                this.listener = Objects.requireNonNull(listener);
                listeners.add(listener);
            }

            @Override
            public void close() {
                if (listener != null) {
                    listeners.remove(listener);
                }
            }
        };
    }

    @Override
    public StoreTransaction begin() {
        lock.lock();
        return new MemoryTransaction(this);
    }

    void unlock() {
        lock.unlock();
    }

    void notifyReady(Set<Integer> topicIds) {
        for (int topicId : topicIds) {
            listeners.forEach(listener -> listener.ready(topicId));
        }
    }

    private static MemoryTransaction memory(StoreTransaction transaction) {
        if (!(transaction instanceof MemoryTransaction memory)) {
            throw new IllegalArgumentException("Not a transaction of this store: " + transaction);
        }
        return memory;
    }

    private Instant now() {
        return clock.instant();
    }

    private void putQueued(MemoryTransaction transaction, Rows.Queued row) {
        transaction.put(queue, row.taskId(), row);
        if (row.state() == State.READY) {
            transaction.ready(row.topicId());
        }
    }

    @Override
    public int createTopic(StoreTransaction transaction, String name) {
        MemoryTransaction memory = memory(transaction);
        OptionalInt existing = findTopic(transaction, name);
        if (existing.isPresent()) {
            return existing.getAsInt();
        }
        int id = nextTopic++;
        memory.put(topics, id, new Rows.Topic(id, name, now()));
        return id;
    }

    @Override
    public OptionalInt findTopic(StoreTransaction transaction, String name) {
        memory(transaction);
        return topics.values().stream().filter(topic -> topic.name().equals(name)).mapToInt(Rows.Topic::id).findFirst();
    }

    @Override
    public List<Pushed> push(StoreTransaction transaction, int topicId, Insertion insertion, List<NewTask> newTasks) throws SQLException {
        MemoryTransaction memory = memory(transaction);
        if (newTasks.isEmpty()) {
            return List.of();
        }
        if (!topics.containsKey(topicId)) {
            throw new SQLException("No topic with id " + topicId);
        }
        Instant now = now();
        Map<String, Integer> last = new HashMap<>();
        for (int index = 0; index < newTasks.size(); index++) {
            last.put(newTasks.get(index).identifier(), index);
        }
        // Every step below sees the queue as it was before this push, like the statement's CTEs.
        Set<Long> before = new HashSet<>(queue.keySet());
        Set<Long> beforeTasks = new HashSet<>(tasks.keySet());
        List<Pushed> results = new ArrayList<>(newTasks.size());
        Set<String> queuedIdentifiers = new LinkedHashSet<>();
        for (int index = 0; index < newTasks.size(); index++) {
            NewTask task = newTasks.get(index);
            boolean superseded = insertion != Insertion.APPEND && last.get(task.identifier()) != index;
            if (superseded && insertion == Insertion.DELETE) {
                results.add(new Pushed(task.identifier(), Pushed.Status.REDUNDANT, null));
                continue;
            }
            long id = nextTask++;
            Rows.Task row = new Rows.Task(topicId, id, task.identifier(), task.reference(), task.payload(), task.payloadType(),
                task.parentId(), task.rootId(), now, null, null, null, null, 0, null);
            if (superseded) {
                memory.put(tasks, id, row.redundant(now));
                results.add(new Pushed(task.identifier(), Pushed.Status.REDUNDANT, id));
                continue;
            }
            if (task.dedupKey() != null && queue.values().stream()
                .anyMatch(queued -> queued.topicId() == topicId && task.dedupKey().equals(queued.dedupKey()))) {
                results.add(new Pushed(task.identifier(), Pushed.Status.DUPLICATE, null));
                continue;
            }
            State state = task.suspended() ? State.SUSPENDED : task.delay() != null && !task.delay().isZero() ? State.SCHEDULED : State.READY;
            memory.put(tasks, id, row);
            putQueued(memory, new Rows.Queued(id, topicId, task.identifier(), state, task.priority(),
                state == State.SCHEDULED ? now.plus(task.delay()) : null, 0, null, null, task.dedupKey()));
            queuedIdentifiers.add(task.identifier());
            results.add(new Pushed(task.identifier(), Pushed.Status.QUEUED, id));
        }
        if (insertion != Insertion.APPEND && !queuedIdentifiers.isEmpty()) {
            Set<State> covered = insertion == Insertion.SUPERSEDE
                ? EnumSet.of(State.READY, State.SCHEDULED, State.RETRYING)
                : EnumSet.complementOf(EnumSet.of(State.ACTIVE));
            for (Rows.Queued old : List.copyOf(queue.values())) {
                if (before.contains(old.taskId()) && old.topicId() == topicId
                    && queuedIdentifiers.contains(old.identifier()) && covered.contains(old.state())) {
                    memory.remove(queue, old.taskId());
                    if (insertion == Insertion.DELETE) {
                        memory.remove(tasks, old.taskId());
                    } else {
                        memory.put(tasks, old.taskId(), tasks.get(old.taskId()).redundant(now));
                    }
                }
            }
            if (insertion != Insertion.SUPERSEDE) {
                for (Rows.Task old : List.copyOf(tasks.values())) {
                    if (beforeTasks.contains(old.id()) && old.topicId() == topicId && queuedIdentifiers.contains(old.identifier())
                        && old.outcome() != null && !queue.containsKey(old.id())) {
                        if (insertion == Insertion.DELETE) {
                            memory.remove(tasks, old.id());
                        } else if (Set.of("succeeded", "filtered", "failed").contains(old.outcome())) {
                            memory.put(tasks, old.id(), old.replaced());
                        }
                    }
                }
            }
        }
        return results;
    }

    @Override
    public Set<String> junction(StoreTransaction transaction, int topicId, Juncture juncture, Collection<Set<String>> groups) {
        MemoryTransaction memory = memory(transaction);
        Set<String> complete = new HashSet<>();
        for (Set<String> group : groups) {
            if (group.stream().allMatch(identifier -> counts(topicId, juncture, identifier))) {
                complete.addAll(group);
            }
        }
        Set<String> released = new LinkedHashSet<>();
        for (Rows.Queued row : List.copyOf(queue.values())) {
            if (row.topicId() == topicId && row.state() == State.SUSPENDED && complete.contains(row.identifier())) {
                putQueued(memory, row.with(State.READY, null, row.attempt(), null, null));
                released.add(row.identifier());
            }
        }
        return released;
    }

    private boolean counts(int topicId, Juncture juncture, String identifier) {
        return switch (juncture) {
            case SINGULAR -> queue.values().stream().anyMatch(row ->
                row.topicId() == topicId && row.identifier().equals(identifier) && row.state() == State.SUSPENDED);
            case ETERNAL -> tasks.values().stream().anyMatch(row ->
                row.topicId() == topicId && row.identifier().equals(identifier) && !"redundant".equals(row.outcome()));
        };
    }

    @Override
    public Claim claim(StoreTransaction transaction, ClaimRequest request) {
        MemoryTransaction memory = memory(transaction);
        Instant now = now();
        Admission admission = request.admission();
        int limit = request.limit();
        double available = 0;
        if (admission != null) {
            Rows.Bucket bucket = bucket(memory, admission.name(), admission.burst());
            available = refilled(bucket, admission, now);
            int permits = (int) Math.min(limit, Math.floor(available));
            if (permits < 1) {
                long waitNanos = (long) Math.ceil((1 - available) / admission.perSecond() * 1_000_000_000L);
                return new Claim(List.of(), Duration.ofNanos(Math.max(waitNanos, 1_000_000L)));
            }
            limit = permits;
        }
        Set<State> blocking = blocking(request.ordering());
        List<Rows.Queued> picked = queue.values().stream()
            .filter(row -> row.topicId() == request.topicId() && row.state() == State.READY)
            .filter(row -> blocking == null || !blocked(row, blocking))
            .sorted(Comparator.comparingInt(Rows.Queued::priority).reversed().thenComparingLong(Rows.Queued::taskId))
            .limit(limit)
            .toList();
        List<ClaimedTask> claimed = new ArrayList<>(picked.size());
        for (Rows.Queued row : picked) {
            Rows.Queued active = row.with(State.ACTIVE, null, row.attempt() + 1, request.nodeId(), now);
            putQueued(memory, active);
            Rows.Task task = tasks.get(row.taskId());
            claimed.add(new ClaimedTask(row.taskId(), row.identifier(), active.attempt(), row.priority(), task.payload(), task.payloadType(),
                task.reference(), task.parentId(), task.rootId(), task.createdAt(), now));
        }
        if (admission != null && !claimed.isEmpty()) {
            Rows.Bucket bucket = buckets.get(admission.name());
            Instant refilledAt = bucket.refilledAt().isAfter(now) ? bucket.refilledAt() : now;
            memory.put(buckets, admission.name(), new Rows.Bucket(admission.name(), Math.max(0, available - claimed.size()), refilledAt));
        }
        return claimed.isEmpty() ? Claim.EMPTY : new Claim(List.copyOf(claimed), null);
    }

    private static Set<State> blocking(Ordering ordering) {
        return switch (ordering) {
            case FIFO -> null;
            case SINGULAR_BY_IDENTIFIER -> EnumSet.of(State.READY, State.RETRYING);
            case SINGULAR_BY_IDENTIFIER_ON_FAILURE -> EnumSet.of(State.READY, State.RETRYING, State.FAILED);
            case SINGULAR_BY_IDENTIFIER_UNTIL_SUCCESS -> EnumSet.of(State.READY, State.RETRYING, State.FAILED, State.SUSPENDED, State.SCHEDULED);
        };
    }

    /** Only tasks in the same topic count; earlier ones in a blocking state, or an active one either way. */
    private boolean blocked(Rows.Queued candidate, Set<State> blocking) {
        return queue.values().stream().anyMatch(other -> other.topicId() == candidate.topicId()
            && other.identifier().equals(candidate.identifier())
            && other.taskId() != candidate.taskId()
            && (other.state() == State.ACTIVE || other.taskId() < candidate.taskId() && blocking.contains(other.state())));
    }

    private Rows.Bucket bucket(MemoryTransaction memory, String name, int burst) {
        Rows.Bucket bucket = buckets.get(name);
        if (bucket == null) {
            bucket = new Rows.Bucket(name, burst, now());
            memory.put(buckets, name, bucket);
        }
        return bucket;
    }

    private static double refilled(Rows.Bucket bucket, Admission admission, Instant now) {
        double elapsed = Math.max(0, Duration.between(bucket.refilledAt(), now).toNanos() / 1e9);
        return Math.min(admission.burst(), bucket.tokens() + admission.perSecond() * elapsed);
    }

    @Override
    public void complete(StoreTransaction transaction, int topicId, int nodeId, List<Completion> completions) throws SQLException {
        Set<Long> matched = apply(memory(transaction), topicId, nodeId, completions);
        if (matched.size() != completions.size()) {
            Set<Long> lost = new LinkedHashSet<>();
            completions.forEach(completion -> {
                if (!matched.contains(completion.taskId())) {
                    lost.add(completion.taskId());
                }
            });
            throw new StaleClaimException(lost);
        }
    }

    @Override
    public int release(StoreTransaction transaction, int topicId, int nodeId, List<Completion> completions) {
        return apply(memory(transaction), topicId, nodeId, completions).size();
    }

    private Set<Long> apply(MemoryTransaction memory, int topicId, int nodeId, List<Completion> completions) {
        Set<Long> unique = new HashSet<>();
        for (Completion completion : completions) {
            if (!unique.add(completion.taskId())) {
                throw new IllegalArgumentException("Task " + completion.taskId() + " is completed twice");
            }
        }
        Instant now = now();
        Set<Long> matched = new LinkedHashSet<>();
        // Finished first, then waiting, like the two statements on Postgres.
        for (boolean finishing : List.of(true, false)) {
            for (Completion completion : completions) {
                if ((completion.resolution() instanceof Resolution.Finished) != finishing) {
                    continue;
                }
                Rows.Queued row = queue.get(completion.taskId());
                if (row == null || row.topicId() != topicId || row.state() != State.ACTIVE
                    || row.attempt() != completion.attempt() || !Objects.equals(row.nodeId(), nodeId)) {
                    continue;
                }
                matched.add(row.taskId());
                Rows.Task task = tasks.get(row.taskId());
                switch (completion.resolution()) {
                    case Resolution.Finished finished -> {
                        memory.remove(queue, row.taskId());
                        String outcome = finished.outcome() == Outcome.SUCCEEDED ? "succeeded" : "filtered";
                        memory.put(tasks, task.id(), task.finish(outcome, finished.output(), row.attempt(), now));
                    }
                    case Resolution.Waiting waiting -> {
                        State state = switch (waiting.state()) {
                            case SCHEDULED -> State.SCHEDULED;
                            case RETRYING -> State.RETRYING;
                            case SUSPENDED -> State.SUSPENDED;
                            case DEAD_LETTER -> State.FAILED;
                        };
                        int attempt = row.attempt() - (waiting.counted() ? 0 : 1);
                        putQueued(memory, row.with(state, waiting.delay() == null ? null : now.plus(waiting.delay()), attempt, null, null));
                        if (state == State.FAILED) {
                            memory.put(tasks, task.id(), task.failed(waiting.error(), attempt));
                        }
                        if (waiting.error() != null) {
                            memory.append(attempts, new Rows.Attempt(topicId, row.taskId(), completion.attempt(), nodeId,
                                completion.claimedAt() == null ? now : completion.claimedAt(), now, "failed", waiting.error()));
                        }
                    }
                }
            }
        }
        return matched;
    }

    @Override
    public List<Integer> promoteDue(StoreTransaction transaction, int limit) {
        MemoryTransaction memory = memory(transaction);
        Instant now = now();
        Set<Integer> promoted = new LinkedHashSet<>();
        queue.values().stream()
            .filter(row -> (row.state() == State.SCHEDULED || row.state() == State.RETRYING) && !row.runAt().isAfter(now))
            .sorted(Comparator.comparing(Rows.Queued::runAt))
            .limit(limit)
            .toList()
            .forEach(row -> {
                putQueued(memory, row.with(State.READY, null, row.attempt(), null, null));
                promoted.add(row.topicId());
            });
        return List.copyOf(promoted);
    }

    @Override
    public List<Integer> requeueExpired(StoreTransaction transaction, int nodeId, Duration expiry) {
        MemoryTransaction memory = memory(transaction);
        Instant now = now();
        Set<Integer> stale = new HashSet<>();
        nodes.values().forEach(node -> {
            if (node.id() != nodeId && node.heartbeatAt().isBefore(now.minus(expiry))) {
                stale.add(node.id());
            }
        });
        Set<Integer> requeued = new LinkedHashSet<>();
        for (Rows.Queued row : List.copyOf(queue.values())) {
            if (row.state() == State.ACTIVE && stale.contains(row.nodeId())) {
                putQueued(memory, row.with(State.READY, null, row.attempt(), null, null));
                memory.append(attempts, new Rows.Attempt(row.topicId(), row.taskId(), row.attempt(), row.nodeId(),
                    row.claimedAt() == null ? now : row.claimedAt(), now, "expired", null));
                requeued.add(row.topicId());
            }
        }
        return List.copyOf(requeued);
    }

    @Override
    public int registerNode(StoreTransaction transaction, String name, String version) {
        MemoryTransaction memory = memory(transaction);
        int id = nextNode++;
        Instant now = now();
        memory.put(nodes, id, new Rows.Node(id, name, version, now, now));
        return id;
    }

    @Override
    public void heartbeat(StoreTransaction transaction, int nodeId) {
        MemoryTransaction memory = memory(transaction);
        Rows.Node node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalStateException("Node " + nodeId + " is not registered");
        }
        memory.put(nodes, nodeId, node.beat(now()));
    }

    @Override
    public void ensureAdmission(StoreTransaction transaction, String name, int burst) {
        bucket(memory(transaction), name, burst);
    }

    @Override
    public Optional<TaskView> find(StoreTransaction transaction, long taskId) {
        memory(transaction);
        Rows.Task task = tasks.get(taskId);
        if (task == null) {
            return Optional.empty();
        }
        Rows.Queued row = queue.get(taskId);
        return Optional.of(new TaskView(
            topics.get(task.topicId()).name(),
            task.id(),
            task.identifier(),
            row != null ? row.state().label() : task.outcome(),
            row == null,
            row != null ? row.attempt() : task.attempts(),
            row != null ? row.runAt() : null,
            row != null ? row.priority() : 0,
            row != null ? row.nodeId() : null,
            task.reference(),
            task.parentId(),
            task.rootId(),
            task.payload(),
            task.payloadType(),
            task.output(),
            task.error(),
            task.previousOutcome(),
            task.createdAt(),
            task.completedAt()
        ));
    }

    @Override
    public QueueCounts count(StoreTransaction transaction, int topicId) {
        memory(transaction);
        Instant now = now();
        long[] counts = new long[State.values().length];
        long due = 0;
        Instant next = null;
        for (Rows.Queued row : queue.values()) {
            if (row.topicId() != topicId) {
                continue;
            }
            counts[row.state().ordinal()]++;
            if (row.runAt() != null) {
                due += row.runAt().isAfter(now) ? 0 : 1;
                next = next == null || row.runAt().isBefore(next) ? row.runAt() : next;
            }
        }
        return new QueueCounts(counts[State.SCHEDULED.ordinal()], counts[State.RETRYING.ordinal()], counts[State.READY.ordinal()],
            counts[State.ACTIVE.ordinal()], counts[State.SUSPENDED.ordinal()], counts[State.FAILED.ordinal()], due, next);
    }

    @Override
    public List<AttemptView> attempts(StoreTransaction transaction, long taskId) {
        memory(transaction);
        return attempts.stream()
            .filter(attempt -> attempt.taskId() == taskId)
            .sorted(Comparator.comparingInt(Rows.Attempt::attempt).thenComparing(Rows.Attempt::finishedAt))
            .map(attempt -> new AttemptView(attempt.taskId(), attempt.attempt(), attempt.nodeId(), attempt.result(), attempt.error()))
            .toList();
    }

    @Override
    public String toString() {
        return "InMemoryTaskStore";
    }
}
