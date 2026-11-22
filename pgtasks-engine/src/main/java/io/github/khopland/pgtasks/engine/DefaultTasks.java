package io.github.khopland.pgtasks.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.PayloadCodec;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TaskStorageException;
import io.github.khopland.pgtasks.TaskWriter;
import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.spi.AppTransaction;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;
import io.github.khopland.pgtasks.spi.TransactionBinding;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction-aware enqueue. Only {@link #independently()} ever commits on its own. With a store
 * that keeps its tasks outside the application's database, {@link #push} holds the pushes back
 * until the application transaction commits.
 */
final class DefaultTasks implements Tasks {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTasks.class);

    /**
     * Sees pushes once they are committed, for tests that capture pushes. Pushes on an explicit
     * connection are seen when written, since their commit is the caller's.
     */
    interface Observer {
        void pushed(TopicRef<?> topic, Insertion insertion, List<? extends Creation<?>> creations, List<Pushed> results);
    }

    private final TaskStore store;
    private final TopicDirectory topics;
    private final Map<String, PayloadCodec<?>> topicCodecs;
    private final List<TopicTemplate<?>> templates;
    private final Map<Class<?>, PayloadCodec<?>> typeCodecs;
    private final TransactionBinding transactions;
    private final Consumer<String> wakeLocally;
    private final TaskObserver taskObserver;
    private volatile @Nullable PayloadCodecs fallbackCodecs;
    private final Map<Class<?>, PayloadCodec<?>> fallbackByType = new ConcurrentHashMap<>();
    private volatile Observer observer = (topic, insertion, creations, results) -> { };

    DefaultTasks(
        TaskStore store,
        TopicDirectory topics,
        Map<String, PayloadCodec<?>> topicCodecs,
        List<TopicTemplate<?>> templates,
        Map<Class<?>, PayloadCodec<?>> typeCodecs,
        TransactionBinding transactions,
        Consumer<String> wakeLocally,
        TaskObserver taskObserver
    ) {
        this.store = store;
        this.topics = topics;
        this.topicCodecs = Map.copyOf(topicCodecs);
        this.templates = List.copyOf(templates);
        this.typeCodecs = Map.copyOf(typeCodecs);
        this.transactions = transactions;
        this.wakeLocally = wakeLocally;
        this.taskObserver = taskObserver;
    }

    void fallbackCodecs(@Nullable PayloadCodecs codecs) {
        this.fallbackCodecs = codecs;
    }

    void observe(Observer observer) {
        this.observer = Objects.requireNonNull(observer);
    }

    /** Pairs of handler topic and target topic already warned about, so each is logged once. */
    private final Set<String> warnedHandlers = ConcurrentHashMap.newKeySet();

    @Override
    public <T> List<Pushed> push(TopicRef<T> topic, Insertion insertion, List<Creation<T>> creations) {
        String handler = HandlerThread.topic();
        AppTransaction transaction = transactions.current().orElseThrow(() -> new IllegalStateException(handler != null
            ? "tasks.push(...) to " + topic + " from a handler of " + handler + ": there is no transaction to join. Stage the push with "
                + "task.push(...) on the task, so it commits with the task's completion and only if the task succeeds, "
                + "or use tasks.independently() for a push of its own."
            : "tasks.push(...) joins the current transaction, but there is none. Run it inside a transaction, "
                + "use tasks.on(connection) to push on an explicit connection, or tasks.independently() to commit on its own."
        ));
        if (handler != null && warnedHandlers.add(handler + " -> " + topic.name())) {
            LOGGER.warn("A handler of {} pushed to {} in a transaction of its own, which commits before the task completes: if the task "
                + "fails or its completion is lost, the retry pushes again. Stage downstream work with task.push(...) unless that is what "
                + "you want. (Logged once.)", handler, topic);
        }
        if (store.joinsApplicationTransactions()) {
            Connection connection = transaction.connection().orElseThrow(() -> new IllegalStateException(
                "The current transaction has no JDBC connection, so a push to " + store + " can't join it."));
            List<Pushed> results = write(join(connection), topic, insertion, creations);
            transaction.afterCommit(() -> observed(topic, insertion, creations, results));
            return results;
        }
        // The store can't join the transaction: check now, store once it has committed.
        List<Creation<T>> held = List.copyOf(creations);
        codec(topic);
        try {
            EngineCore.transact(store, storeTransaction -> topics.id(storeTransaction, topic.name()));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        transaction.afterCommit(() -> {
            try {
                independently().push(topic, insertion, held);
            } catch (RuntimeException e) {
                LOGGER.error("Could not store pushes to {} after the transaction committed", topic, e);
                throw e;
            }
        });
        return held.stream().map(creation -> new Pushed(creation.identifier(), Pushed.Status.PENDING, null)).toList();
    }

    @Override
    public TaskWriter on(Connection connection) {
        Objects.requireNonNull(connection, "connection");
        if (!store.joinsApplicationTransactions()) {
            throw new UnsupportedOperationException(store + " keeps its tasks outside your database, so a push can't join a JDBC "
                + "transaction. Use tasks.push(...) inside a transaction, which holds the push back until it commits, "
                + "or tasks.independently().");
        }
        return new TaskWriter() {
            @Override
            public <T> List<Pushed> push(TopicRef<T> topic, Insertion insertion, List<Creation<T>> creations) {
                List<Pushed> results = write(join(connection), topic, insertion, creations);
                observed(topic, insertion, creations, results);
                return results;
            }
        };
    }

    @Override
    public TaskWriter independently() {
        return new TaskWriter() {
            @Override
            public <T> List<Pushed> push(TopicRef<T> topic, Insertion insertion, List<Creation<T>> creations) {
                List<Pushed> pushed;
                try {
                    pushed = EngineCore.transact(store, transaction -> write(transaction, topic, insertion, creations));
                } catch (SQLException e) {
                    throw new TaskStorageException("Could not push to " + topic, e);
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                observed(topic, insertion, creations, pushed);
                wakeLocally.accept(topic.name());
                return pushed;
            }
        };
    }

    private StoreTransaction join(Connection connection) {
        return store.join(connection);
    }

    void observed(TopicRef<?> topic, Insertion insertion, List<? extends Creation<?>> creations, List<Pushed> results) {
        taskObserver.pushed(topic, results);
        try {
            observer.pushed(topic, insertion, creations, results);
        } catch (RuntimeException e) {
            LOGGER.warn("A push observer failed", e);
        }
    }

    private <T> List<Pushed> write(StoreTransaction transaction, TopicRef<T> topic, Insertion insertion, List<Creation<T>> creations) {
        return write(transaction, topic, insertion, creations, null, parent -> TaskObserver.Scope.NONE);
    }

    /**
     * Writes in the given store transaction without committing. {@code parents}, if given, holds
     * the task that staged each creation, for the lineage columns; {@code context} opens a parent
     * task's observation, so the observers see each parent's downstream work pushed from it.
     */
    <T> List<Pushed> write(StoreTransaction transaction, TopicRef<T> topic, Insertion insertion, List<Creation<T>> creations,
                           @Nullable List<? extends Task<?>> parents, Function<Task<?>, TaskObserver.Scope> context) {
        try {
            return store(transaction, topic, insertion, creations, parents, context);
        } catch (SQLException e) {
            throw new TaskStorageException("Could not push to " + topic, e);
        }
    }

    private <T> List<Pushed> store(StoreTransaction transaction, TopicRef<T> topic, Insertion insertion, List<Creation<T>> creations,
                                   @Nullable List<? extends Task<?>> parents, Function<Task<?>, TaskObserver.Scope> context) throws SQLException {
        if (creations.isEmpty()) {
            return List.of();
        }
        PayloadCodec<T> codec = codec(topic);
        int topicId = topics.id(transaction, topic.name());
        List<TaskObserver.PushObservation> observations = new ArrayList<>();
        try {
            List<Map<String, String>> headers = observe(topic, creations.size(), parents, context, observations);
            return store.push(transaction, topicId, insertion, newTasks(codec, creations, parents, headers));
        } finally {
            observations.reversed().forEach(TaskObserver.PushObservation::close);
        }
    }

    /**
     * Starts the push's observations, each closed by the caller once the write is done: one for
     * the push, or one per parent task, started with that task opened. Returns the headers the
     * observers give each creation.
     */
    private List<Map<String, String>> observe(TopicRef<?> topic, int size, @Nullable List<? extends Task<?>> parents,
                                              Function<Task<?>, TaskObserver.Scope> context, List<TaskObserver.PushObservation> observations) {
        if (parents == null) {
            TaskObserver.PushObservation observation = taskObserver.pushing(topic, size);
            observations.add(observation);
            return Collections.nCopies(size, observation.headers());
        }
        Map<Long, List<Integer>> byParent = new LinkedHashMap<>();
        for (int index = 0; index < size; index++) {
            byParent.computeIfAbsent(parents.get(index).id(), id -> new ArrayList<>()).add(index);
        }
        List<Map<String, String>> headers = new ArrayList<>(Collections.nCopies(size, Map.of()));
        for (List<Integer> indexes : byParent.values()) {
            try (TaskObserver.Scope ignored = context.apply(parents.get(indexes.getFirst()))) {
                TaskObserver.PushObservation observation = taskObserver.pushing(topic, indexes.size());
                observations.add(observation);
                Map<String, String> observed = observation.headers();
                indexes.forEach(index -> headers.set(index, observed));
            }
        }
        return headers;
    }

    private static <T> List<TaskStore.NewTask> newTasks(PayloadCodec<T> codec, List<Creation<T>> creations,
                                                       @Nullable List<? extends Task<?>> parents, List<Map<String, String>> observed) {
        List<TaskStore.NewTask> tasks = new ArrayList<>(creations.size());
        for (int index = 0; index < creations.size(); index++) {
            Creation<T> creation = creations.get(index);
            Task<?> parent = parents == null ? null : parents.get(index);
            Map<String, String> headers = observed.get(index);
            if (!creation.headers().isEmpty()) {
                headers = new LinkedHashMap<>(headers);
                headers.putAll(creation.headers());
            }
            tasks.add(new TaskStore.NewTask(
                creation.identifier(),
                creation.payload() == null ? null : codec.encode(creation.payload()),
                codec.type(),
                creation.reference(),
                creation.delay(),
                creation.suspended(),
                creation.priority(),
                creation.dedupKey(),
                parent == null ? null : parent.id(),
                parent == null ? null : parent.flowRootId(),
                headers
            ));
        }
        return tasks;
    }

    /**
     * Fails now, in the handler, if downstream work for this topic couldn't be written at the
     * completion: no codec for its payloads, or no such topic. Otherwise one task's mistake would
     * fail the whole batch's completion. The topic is looked up once, then known.
     */
    void checkTarget(TopicRef<?> topic, boolean needsCodec) {
        if (needsCodec) {
            codec(topic);
        }
        if (topics.known(topic.name())) {
            return;
        }
        try {
            EngineCore.transact(store, transaction -> topics.id(transaction, topic.name()));
        } catch (SQLException e) {
            throw new TaskStorageException("Could not look up topic " + topic, e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    int topicId(StoreTransaction transaction, String topic) throws SQLException {
        return topics.id(transaction, topic);
    }

    @SuppressWarnings("unchecked")
    <T> PayloadCodec<T> codec(TopicRef<T> topic) {
        PayloadCodec<?> codec = topicCodecs.get(topic.name());
        if (codec == null) {
            codec = templates.stream().filter(template -> template.matches(topic.name())).map(TopicTemplate::codec).findFirst().orElse(null);
        }
        if (codec == null) {
            codec = typeCodecs.get(topic.payloadType());
        }
        if (codec == null && topic.payloadType() == String.class) {
            codec = PayloadCodec.string();
        }
        PayloadCodecs fallback = fallbackCodecs;
        if (codec == null && fallback != null) {
            codec = fallbackByType.computeIfAbsent(topic.payloadType(), fallback::codecFor);
        }
        if (codec == null) {
            throw new IllegalArgumentException("No payload codec for " + topic.payloadType().getName() + " (topic " + topic + ")");
        }
        return (PayloadCodec<T>) codec;
    }
}
