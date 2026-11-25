package io.github.khopland.pgtasks.memory;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.time.Period;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.admin.NodeView;
import io.github.khopland.pgtasks.admin.DeadLetterQuery;
import io.github.khopland.pgtasks.admin.QueueCounts;
import io.github.khopland.pgtasks.admin.QueueState;
import io.github.khopland.pgtasks.admin.TaskQuery;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.admin.TopicSnapshot;
import io.github.khopland.pgtasks.memory.Rows.State;
import io.github.khopland.pgtasks.spi.Notifier;
import io.github.khopland.pgtasks.spi.StaleClaimException;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.jspecify.annotations.Nullable;

/**
 * Tasks in memory, for running an app locally without a database and for fast tests. It passes
 * the same TCK as the Postgres store, so ordering, insertion policies, fencing and retries behave
 * the same way. Several engines in one JVM can share one instance; {@link #notifier()} gives each
 * of them in-process wake-ups.
 *
 * <p>Limits: nothing survives a restart; wake-ups only reach engines in this JVM; and the store
 * can't share a transaction with the application's own tables, so pushes made in an application
 * transaction are applied after it commits. Atomicity with the application's data can only be
 * proven on Postgres. It is never chosen automatically.
 *
 * <p>Every transaction holds one store-wide lock from begin to commit or rollback, so
 * transactions on different threads never interleave. So task ids always follow commit order and
 * claims always see each other, which Postgres has to arrange with advisory locks: per identifier
 * for pushes other than APPEND, and per topic for SINGULAR claims. Concurrent APPEND pushes for
 * one identifier, which Postgres may commit out of id order, are never reordered here. Time comes from the given clock; pass a
 * {@code ManualClock} to jump past backoffs, delays and expiry in tests.
 */
public final class InMemoryTaskStore implements TaskStore {

    private final InstantSource clock;
    private final ReentrantLock lock = new ReentrantLock(true);
    /** Each engine's listener, with the channels it listens to. */
    private final Map<Notifier.Listener, Set<String>> listeners = new ConcurrentHashMap<>();

    private final Map<Integer, Rows.Topic> topics = new LinkedHashMap<>();
    private final Map<Integer, Rows.Node> nodes = new HashMap<>();
    private final TreeMap<Long, Rows.Task> tasks = new TreeMap<>();
    private final TreeMap<Long, Rows.Queued> queue = new TreeMap<>();
    private final Map<Integer, TopicSnapshot> snapshots = new HashMap<>();
    private final Map<String, Held> leases = new HashMap<>();
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

    /** In-process wake-ups for one engine; every engine sharing this store gets its own. */
    @Override
    public Notifier notifier() {
        return new Notifier() {
            private @Nullable Listener listener;

            @Override
            public void start(Listener listener, Set<String> channels) {
                this.listener = Objects.requireNonNull(listener);
                listeners.put(listener, Set.copyOf(channels));
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
            listeners.keySet().forEach(listener -> listener.ready(topicId));
        }
    }

    /** Like LISTEN/NOTIFY: each message reaches the listeners of its channel, once committed. */
    void deliver(Set<MemoryTransaction.Message> messages) {
        for (MemoryTransaction.Message message : messages) {
            listeners.forEach((listener, channels) -> {
                if (channels.contains(message.channel())) {
                    listener.notified(message.channel(), message.payload());
                }
            });
        }
    }

    @Override
    public void notify(StoreTransaction transaction, String channel, String payload) {
        if (!Notifier.CHANNEL.matcher(channel).matches() || channel.equals("ready")) {
            throw new IllegalArgumentException("Invalid channel '" + channel + "': a lowercase word other than ready");
        }
        memory(transaction).send(channel, payload);
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
        memory.put(topics, id, new Rows.Topic(id, name, now(), null));
        memory.send(Notifier.TOPIC, name);
        return id;
    }

    @Override
    public List<TopicInfo> topics(StoreTransaction transaction) {
        memory(transaction);
        return topics.values().stream().map(topic -> new TopicInfo(topic.id(), topic.name())).toList();
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
            throw new SQLException("No topic with id " + topicId, "23503"); // like the foreign key on task.topic_id
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
            boolean superseded = insertion != Insertion.APPEND && last.getOrDefault(task.identifier(), index) != index;
            long id = nextTask++;
            Rows.Task row = new Rows.Task(topicId, id, task.identifier(), task.reference(), task.payload(), task.payloadType(),
                task.parentId(), task.rootId(), now, null, null, null, null, 0, null, task.headers());
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
            // A run time that has come is ready at once, as a zero delay is.
            Instant runAt = task.runAt() != null ? task.runAt() : task.delay() != null && !task.delay().isZero() ? now.plus(task.delay()) : null;
            State state = task.suspended() ? State.SUSPENDED : runAt != null && runAt.isAfter(now) ? State.SCHEDULED : State.READY;
            memory.put(tasks, id, row);
            putQueued(memory, new Rows.Queued(id, topicId, task.identifier(), state, task.priority(),
                state == State.SCHEDULED ? runAt : null, 0, null, null, task.dedupKey(), 0, null, 0));
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
                    memory.put(tasks, old.taskId(), task(old.taskId()).redundant(now));
                }
            }
            if (insertion != Insertion.SUPERSEDE) {
                for (Rows.Task old : List.copyOf(tasks.values())) {
                    if (beforeTasks.contains(old.id()) && old.topicId() == topicId && queuedIdentifiers.contains(old.identifier())
                        && old.outcome() != null && !queue.containsKey(old.id())
                        && Set.of("succeeded", "filtered", "failed").contains(old.outcome())) {
                        memory.put(tasks, old.id(), old.replaced());
                    }
                }
            }
        }
        return results;
    }

    @Override
    public Claim claim(StoreTransaction transaction, ClaimRequest request) {
        MemoryTransaction memory = memory(transaction);
        Instant now = now();
        Set<State> blocking = blocking(request.ordering());
        List<Rows.Queued> picked = queue.values().stream()
            .filter(row -> row.topicId() == request.topicId() && row.state() == State.READY)
            .filter(row -> blocking == null || !blocked(row, blocking))
            .sorted(Comparator.comparingInt(Rows.Queued::priority).reversed().thenComparingLong(Rows.Queued::taskId))
            .limit(request.limit())
            .toList();
        List<ClaimedTask> claimed = new ArrayList<>(picked.size());
        for (Rows.Queued row : picked) {
            Rows.Queued active = row.with(State.ACTIVE, null, row.attempt() + 1, request.nodeId(), now);
            putQueued(memory, active);
            Rows.Task task = task(row.taskId());
            claimed.add(new ClaimedTask(row.taskId(), row.identifier(), active.attempt(), row.priority(), task.payload(), task.payloadType(),
                task.reference(), task.parentId(), task.rootId(), task.createdAt(), now, task.headers(), row.failures(), row.firstFailedAt()));
        }
        return claimed.isEmpty() ? Claim.EMPTY : new Claim(List.copyOf(claimed));
    }

    private static @Nullable Set<State> blocking(Ordering ordering) {
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
            && (other.state() == State.ACTIVE || (other.taskId() < candidate.taskId() && blocking.contains(other.state()))));
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

    @Override
    public int noteError(StoreTransaction transaction, int topicId, int nodeId, List<ActiveClaim> claims, String error) {
        MemoryTransaction memory = memory(transaction);
        int marked = 0;
        for (ActiveClaim claim : claims) {
            Rows.Queued row = queue.get(claim.taskId());
            if (row == null || row.topicId() != topicId || row.state() != State.ACTIVE
                || row.attempt() != claim.attempt() || !Objects.equals(row.nodeId(), nodeId)) {
                continue;
            }
            Rows.Task task = task(row.taskId());
            memory.put(tasks, task.id(), task.failed(error, task.attempts()));
            marked++;
        }
        return marked;
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
                Rows.Task task = task(row.taskId());
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
                        Rows.Queued waited = row.with(state, waiting.delay() == null ? null : now.plus(waiting.delay()), attempt, null, null);
                        // A run that ends waiting by its own decision didn't fail: the failures in a row end
                        // there. A release (no error, retrying) isn't the end of a run, and keeps them.
                        boolean decidedToWait = state == State.SCHEDULED || state == State.SUSPENDED;
                        putQueued(memory, waiting.error() != null ? waited.failedAt(now) : decidedToWait ? waited.withoutFailureStreak() : waited);
                        if (waiting.error() != null) {
                            // The last error stays on the task from the first failure on, as on Postgres.
                            memory.put(tasks, task.id(), task.failed(waiting.error(), attempt));
                        }
                    }
                }
            }
        }
        return matched;
    }

    @Override
    public Promoted promoteDue(StoreTransaction transaction, int limit) {
        MemoryTransaction memory = memory(transaction);
        Instant now = now();
        Set<Integer> topics = new LinkedHashSet<>();
        List<Rows.Queued> due = queue.values().stream()
            .filter(row -> (row.state() == State.SCHEDULED || row.state() == State.RETRYING) && !Objects.requireNonNull(row.runAt()).isAfter(now))
            .sorted(Comparator.comparing(Rows.Queued::runAt))
            .limit(limit)
            .toList();
        due.forEach(row -> {
            putQueued(memory, row.with(State.READY, null, row.attempt(), null, null));
            topics.add(row.topicId());
        });
        return new Promoted(due.size(), List.copyOf(topics));
    }

    @Override
    public List<Integer> requeueExpired(StoreTransaction transaction, int nodeId, Duration expiry, Expiries expiries) {
        MemoryTransaction memory = memory(transaction);
        Instant now = now();
        Set<Integer> stale = new HashSet<>();
        nodes.values().forEach(node -> {
            if (node.id() != nodeId && node.heartbeatAt().isBefore(now.minus(node.expiry() != null ? node.expiry() : expiry))) {
                stale.add(node.id());
            }
        });
        Set<Integer> requeued = new LinkedHashSet<>();
        for (Rows.Queued row : List.copyOf(queue.values())) {
            if (row.state() == State.ACTIVE && stale.contains(row.nodeId())) {
                boolean dead = row.expiries() + 1 >= expiries.deadLetterAt();
                boolean ready = !dead && expiries.backoff().isZero();
                Instant runAt = dead || ready ? null : now.plus(expiries.backoff().multipliedBy(1L << Math.min(row.expiries(), 20)));
                putQueued(memory, row.with(dead ? State.FAILED : ready ? State.READY : State.RETRYING, runAt, row.attempt(), null, null).expiredAt(now));
                Rows.Task task = task(row.taskId());
                memory.put(tasks, task.id(), task.failed("{\"message\":\"" + (dead ? "Expired " + (row.expiries() + 1)
                    + " times: the node running it stopped every time" : "Expired: the node running it stopped") + "\",\"type\":\"expired\"}", row.attempt()));
                requeued.add(row.topicId());
            }
        }
        return List.copyOf(requeued);
    }

    /** A lease: who holds it and until when, in store time. */
    private record Held(int nodeId, Instant until) {
    }

    @Override
    public boolean lead(StoreTransaction transaction, String name, int nodeId, Duration term) {
        MemoryTransaction memory = memory(transaction);
        Instant now = now();
        Held current = leases.get(name);
        if (current != null && current.nodeId() != nodeId && !current.until().isBefore(now)) {
            return false;
        }
        memory.put(leases, name, new Held(nodeId, now.plus(term)));
        return true;
    }

    @Override
    public void resign(StoreTransaction transaction, String name, int nodeId) {
        MemoryTransaction memory = memory(transaction);
        Held current = leases.get(name);
        if (current != null && current.nodeId() == nodeId) {
            memory.remove(leases, name);
        }
    }

    @Override
    public int purge(StoreTransaction transaction, int topicId, io.github.khopland.pgtasks.Outcome outcome, Instant completedBefore, int limit) {
        MemoryTransaction memory = memory(transaction);
        List<Rows.Task> purged = tasks.values().stream()
            .filter(task -> task.topicId() == topicId && outcome.label().equals(task.outcome()) && !queue.containsKey(task.id())
                && task.completedAt() != null && task.completedAt().isBefore(completedBefore))
            .sorted(Comparator.comparing(Rows.Task::completedAt))
            .limit(limit)
            .toList();
        purged.forEach(task -> deleteTask(memory, task.id()));
        return purged.size();
    }

    @Override
    public int purgeAll(StoreTransaction transaction, int topicId, int limit) {
        MemoryTransaction memory = memory(transaction);
        List<Rows.Task> purged = tasks.values().stream()
            .filter(task -> task.topicId() == topicId && task.outcome() != null && !queue.containsKey(task.id()))
            .limit(limit)
            .toList();
        purged.forEach(task -> deleteTask(memory, task.id()));
        return purged.size();
    }

    @Override
    public boolean deleteTopic(StoreTransaction transaction, int topicId) {
        MemoryTransaction memory = memory(transaction);
        Rows.Topic topic = topics.get(topicId);
        if (topic == null || tasks.values().stream().anyMatch(task -> task.topicId() == topicId)) {
            return false;
        }
        memory.remove(snapshots, topicId);
        memory.remove(topics, topicId);
        memory.send(Notifier.TOPIC, topic.name());
        return true;
    }

    @Override
    public void setRetention(StoreTransaction transaction, int topicId, Map<io.github.khopland.pgtasks.Outcome, Period> retention) throws SQLException {
        MemoryTransaction memory = memory(transaction);
        Rows.Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new SQLException("No topic with id " + topicId);
        }
        memory.put(topics, topicId, topic.withRetention(retention));
    }

    @Override
    public Map<Integer, Map<io.github.khopland.pgtasks.Outcome, Period>> retention(StoreTransaction transaction) {
        memory(transaction);
        Map<Integer, Map<io.github.khopland.pgtasks.Outcome, Period>> result = new LinkedHashMap<>();
        topics.values().stream()
            .filter(topic -> topic.retention() != null && !topic.retention().isEmpty())
            .forEach(topic -> result.put(topic.id(), topic.retention()));
        return result;
    }

    @Override
    public void snapshotCounts(StoreTransaction transaction) {
        MemoryTransaction memory = memory(transaction);
        Instant now = now();
        for (Rows.Topic topic : List.copyOf(topics.values())) {
            memory.put(snapshots, topic.id(), new TopicSnapshot(topic.name(), count(transaction, topic.id()), now));
        }
    }

    @Override
    public List<TopicSnapshot> snapshots(StoreTransaction transaction) {
        memory(transaction);
        return snapshots.values().stream().sorted(Comparator.comparing(TopicSnapshot::topic)).toList();
    }

    @Override
    public List<ActiveClaim> activeClaims(StoreTransaction transaction, int nodeId) {
        memory(transaction);
        return queue.values().stream()
            .filter(row -> row.state() == State.ACTIVE && Objects.equals(row.nodeId(), nodeId))
            .map(row -> new ActiveClaim(row.topicId(), row.taskId(), row.attempt(), Objects.requireNonNull(row.claimedAt())))
            .toList();
    }

    @Override
    public int registerNode(StoreTransaction transaction, String name, @Nullable String version, @Nullable Duration expiry) {
        if (expiry != null && !expiry.isPositive()) {
            throw new IllegalArgumentException("A node's expiry must be positive, not " + expiry);
        }
        MemoryTransaction memory = memory(transaction);
        int id = nextNode++;
        Instant now = now();
        memory.put(nodes, id, new Rows.Node(id, name, version, now, now, expiry));
        return id;
    }

    @Override
    public boolean heartbeat(StoreTransaction transaction, int nodeId) {
        MemoryTransaction memory = memory(transaction);
        Rows.Node node = nodes.get(nodeId);
        if (node == null) {
            return false;
        }
        memory.put(nodes, nodeId, node.beat(now()));
        return true;
    }

    /** A task that must exist, such as the task of a queued row. */
    private Rows.Task task(long id) {
        return Objects.requireNonNull(tasks.get(id), () -> "No task " + id);
    }

    /** A topic that must exist, such as the topic of a stored row. */
    private Rows.Topic topic(int id) {
        return Objects.requireNonNull(topics.get(id), () -> "No topic " + id);
    }

    private void deleteTask(MemoryTransaction memory, long taskId) {
        memory.remove(tasks, taskId);
    }

    @Override
    public List<TaskView> deadLetters(StoreTransaction transaction, int topicId, DeadLetterQuery query, int limit) {
        return failed(topicId, query, limit).stream()
            .map(row -> find(transaction, row.taskId()).orElseThrow())
            .toList();
    }

    @Override
    public List<Long> revive(StoreTransaction transaction, int topicId, DeadLetterQuery query, int limit) {
        MemoryTransaction memory = memory(transaction);
        List<Rows.Queued> revived = failed(topicId, query, limit);
        revived.forEach(row -> putQueued(memory, row.with(State.READY, null, 0, null, null).withoutFailures()));
        return revived.stream().map(Rows.Queued::taskId).toList();
    }

    @Override
    public List<Long> discard(StoreTransaction transaction, int topicId, DeadLetterQuery query, int limit) {
        MemoryTransaction memory = memory(transaction);
        List<Rows.Queued> discarded = failed(topicId, query, limit);
        Instant now = now();
        for (Rows.Queued row : discarded) {
            memory.remove(queue, row.taskId());
            Rows.Task task = task(row.taskId());
            memory.put(tasks, task.id(), task.finish("failed", task.output(), row.attempt(), now));
        }
        if (!discarded.isEmpty()) {
            memory.ready(topicId);
        }
        return discarded.stream().map(Rows.Queued::taskId).toList();
    }

    @Override
    public List<TaskView> tasks(StoreTransaction transaction, int topicId, TaskQuery query, int limit) {
        return unfinished(topicId, query, EnumSet.allOf(State.class), limit).stream()
            .map(row -> find(transaction, row.taskId()).orElseThrow())
            .toList();
    }

    @Override
    public List<Long> releaseSuspended(StoreTransaction transaction, int topicId, TaskQuery query, int limit) {
        MemoryTransaction memory = memory(transaction);
        List<Rows.Queued> released = unfinished(topicId, query.inStates(QueueState.SUSPENDED), EnumSet.of(State.SUSPENDED), limit);
        released.forEach(row -> putQueued(memory, row.with(State.READY, null, row.attempt(), null, null)));
        return released.stream().map(Rows.Queued::taskId).toList();
    }

    /** The topic's unfinished tasks in these states that match, lowest id first. */
    private List<Rows.Queued> unfinished(int topicId, TaskQuery query, Set<State> states, int limit) {
        return queue.values().stream()
            .filter(row -> row.topicId() == topicId && states.contains(row.state()))
            .filter(row -> {
                Rows.Task task = task(row.taskId());
                return query.matches(row.taskId(), row.identifier(), task.reference(), QueueState.valueOf(row.state().name()), task.headers(),
                    task.createdAt());
            })
            .sorted(Comparator.comparingLong(Rows.Queued::taskId))
            .limit(limit)
            .toList();
    }

    /** The topic's dead letters, lowest id first: the given ones, or all, after an id. */
    private List<Rows.Queued> failed(int topicId, DeadLetterQuery query, int limit) {
        Set<Long> wanted = query.taskIds() == null ? null : Set.copyOf(query.taskIds());
        return queue.values().stream()
            .filter(row -> row.topicId() == topicId && row.state() == State.FAILED && row.taskId() > query.afterId())
            .filter(row -> wanted == null || wanted.contains(row.taskId()))
            .filter(row -> query.error() == null || query.error().equals(errorKey(task(row.taskId()).error())))
            .filter(row -> query.failedFrom() == null || (row.firstFailedAt() != null && !row.firstFailedAt().isBefore(query.failedFrom())))
            .filter(row -> query.failedUntil() == null || (row.firstFailedAt() != null && row.firstFailedAt().isBefore(query.failedUntil())))
            .sorted(Comparator.comparingLong(Rows.Queued::taskId))
            .limit(limit)
            .toList();
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
            topic(task.topicId()).name(),
            task.id(),
            task.identifier(),
            row != null ? row.state().label() : Objects.requireNonNull(task.outcome()),
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
            task.completedAt(),
            row != null ? row.failures() : 0,
            row != null ? row.firstFailedAt() : null,
            task.headers()
        ));
    }

    @Override
    public QueueCounts count(StoreTransaction transaction, int topicId) {
        memory(transaction);
        Instant now = now();
        Map<State, Long> counts = new EnumMap<>(State.class);
        long due = 0;
        Instant next = null;
        Long oldestReady = null;
        long failing = 0;
        Instant oldestFailing = null;
        for (Rows.Queued row : queue.values()) {
            if (row.topicId() != topicId) {
                continue;
            }
            counts.merge(row.state(), 1L, Long::sum);
            if (row.failures() > 0) {
                failing++;
                Instant failedAt = Objects.requireNonNull(row.firstFailedAt());
                oldestFailing = oldestFailing == null || failedAt.isBefore(oldestFailing) ? failedAt : oldestFailing;
            }
            if (row.state() == State.READY && (oldestReady == null || row.taskId() < oldestReady)) {
                oldestReady = row.taskId();
            }
            if (row.runAt() != null) {
                due += row.runAt().isAfter(now) ? 0 : 1;
                next = next == null || row.runAt().isBefore(next) ? row.runAt() : next;
            }
        }
        return new QueueCounts(counts.getOrDefault(State.SCHEDULED, 0L), counts.getOrDefault(State.RETRYING, 0L),
            counts.getOrDefault(State.READY, 0L), counts.getOrDefault(State.ACTIVE, 0L), counts.getOrDefault(State.SUSPENDED, 0L),
            counts.getOrDefault(State.FAILED, 0L), due, next, oldestReady == null ? null : task(oldestReady).createdAt(), failing, oldestFailing);
    }

    @Override
    public int deleteDeadNodes(StoreTransaction transaction, Duration deadFor) {
        MemoryTransaction memory = memory(transaction);
        Instant cutoff = now().minus(deadFor);
        Set<Integer> claiming = new HashSet<>();
        queue.values().forEach(row -> {
            if (row.nodeId() != null) {
                claiming.add(row.nodeId());
            }
        });
        List<Rows.Node> dead = nodes.values().stream()
            .filter(node -> node.heartbeatAt().isBefore(cutoff) && !claiming.contains(node.id()))
            .toList();
        dead.forEach(node -> memory.remove(nodes, node.id()));
        return dead.size();
    }

    @Override
    public boolean deregisterNode(StoreTransaction transaction, int nodeId) {
        MemoryTransaction memory = memory(transaction);
        boolean claiming = queue.values().stream().anyMatch(row -> Integer.valueOf(nodeId).equals(row.nodeId()));
        if (claiming || !nodes.containsKey(nodeId)) {
            return false;
        }
        memory.remove(nodes, nodeId);
        return true;
    }

    /** The error's type, or its message if it has none, as Postgres groups them. */
    private static String errorKey(@Nullable String error) {
        String type = jsonField(error, "type");
        if (type != null) {
            return type;
        }
        String message = jsonField(error, "message");
        return message != null ? message : "unknown";
    }

    /** A string field of the flat JSON the engine writes; enough for grouping, not a JSON parser. */
    private static @Nullable String jsonField(@Nullable String json, String name) {
        if (json == null) {
            return null;
        }
        Matcher matcher = Pattern.compile("\"" + name + "\":\"((?:[^\"\\\\]|\\\\.)*)\"").matcher(json);
        return matcher.find() ? matcher.group(1).replace("\\\"", "\"").replace("\\n", "\n").replace("\\\\", "\\") : null;
    }

    @Override
    public List<NodeView> nodes(StoreTransaction transaction) {
        memory(transaction);
        Instant now = now();
        return nodes.values().stream()
            .sorted(Comparator.comparingInt(Rows.Node::id))
            .map(node -> new NodeView(node.id(), node.name(), node.version(), node.startedAt(), node.heartbeatAt(),
                leases.entrySet().stream()
                    .filter(lease -> lease.getValue().nodeId() == node.id() && !lease.getValue().until().isBefore(now))
                    .map(Map.Entry::getKey).sorted().toList()))
            .toList();
    }

    @Override
    public String toString() {
        return "InMemoryTaskStore";
    }
}
