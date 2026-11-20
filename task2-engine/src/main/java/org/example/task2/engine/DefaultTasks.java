package org.example.task2.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.example.task2.Creation;
import org.example.task2.Insertion;
import org.example.task2.PayloadCodec;
import org.example.task2.Pushed;
import org.example.task2.Task;
import org.example.task2.TaskStorageException;
import org.example.task2.TaskWriter;
import org.example.task2.Tasks;
import org.example.task2.TopicRef;
import org.example.task2.spi.AppTransaction;
import org.example.task2.spi.StoreTransaction;
import org.example.task2.spi.TaskStore;
import org.example.task2.spi.TransactionBinding;

/**
 * Transaction-aware enqueue. Only {@link #independently()} ever commits on its own. With a store
 * that keeps its tasks outside the application's database, {@link #push} holds the pushes back
 * until the application transaction commits.
 */
final class DefaultTasks implements Tasks {

    private static final System.Logger LOGGER = System.getLogger(DefaultTasks.class.getName());

    /** Sees every push that is stored, for tests that capture pushes. */
    interface Observer {
        void pushed(TopicRef<?> topic, Insertion insertion, List<? extends Creation<?>> creations, List<Pushed> results);
    }

    private final TaskStore store;
    private final TopicDirectory topics;
    private final Map<String, PayloadCodec<?>> topicCodecs;
    private final Map<Class<?>, PayloadCodec<?>> typeCodecs;
    private final TransactionBinding transactions;
    private final Consumer<String> wakeLocally;
    private volatile Observer observer = (topic, insertion, creations, results) -> { };

    DefaultTasks(
        TaskStore store,
        TopicDirectory topics,
        Map<String, PayloadCodec<?>> topicCodecs,
        Map<Class<?>, PayloadCodec<?>> typeCodecs,
        TransactionBinding transactions,
        Consumer<String> wakeLocally
    ) {
        this.store = store;
        this.topics = topics;
        this.topicCodecs = Map.copyOf(topicCodecs);
        this.typeCodecs = Map.copyOf(typeCodecs);
        this.transactions = transactions;
        this.wakeLocally = wakeLocally;
    }

    void observe(Observer observer) {
        this.observer = Objects.requireNonNull(observer);
    }

    @Override
    public <T> List<Pushed> push(TopicRef<T> topic, Insertion insertion, List<Creation<T>> creations) {
        AppTransaction transaction = transactions.current().orElseThrow(() -> new IllegalStateException(
            "tasks.push(...) joins the current transaction, but there is none. Run it inside a transaction, "
                + "use tasks.on(connection) to push on an explicit connection, or tasks.independently() to commit on its own."
        ));
        if (store.joinsApplicationTransactions()) {
            Connection connection = transaction.connection().orElseThrow(() -> new IllegalStateException(
                "The current transaction has no JDBC connection, so a push to " + store + " can't join it."));
            return pushOn(connection, topic, insertion, creations);
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
                LOGGER.log(System.Logger.Level.ERROR, "Could not store pushes to " + topic + " after the transaction committed", e);
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
                return pushOn(connection, topic, insertion, creations);
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
                    pushed = EngineCore.transact(store, transaction -> write(transaction, topic, insertion, creations, null));
                } catch (SQLException e) {
                    throw new TaskStorageException("Could not push to " + topic, e);
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                wakeLocally.accept(topic.name());
                return pushed;
            }
        };
    }

    private <T> List<Pushed> pushOn(Connection connection, TopicRef<T> topic, Insertion insertion, List<Creation<T>> creations) {
        try {
            return write(store.join(connection), topic, insertion, creations, null);
        } catch (SQLException e) {
            throw new TaskStorageException("Could not push to " + topic, e);
        }
    }

    /**
     * Writes in the given store transaction without committing. {@code parents}, if given, holds
     * the task that staged each creation, for the lineage columns.
     */
    <T> List<Pushed> write(StoreTransaction transaction, TopicRef<T> topic, Insertion insertion, List<Creation<T>> creations, List<? extends Task<?>> parents) throws SQLException {
        if (creations.isEmpty()) {
            return List.of();
        }
        PayloadCodec<T> codec = codec(topic);
        int topicId = topics.id(transaction, topic.name());
        List<TaskStore.NewTask> tasks = new ArrayList<>(creations.size());
        for (int index = 0; index < creations.size(); index++) {
            Creation<T> creation = creations.get(index);
            Task<?> parent = parents == null ? null : parents.get(index);
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
                parent == null ? null : parent.flowRootId()
            ));
        }
        List<Pushed> results = store.push(transaction, topicId, insertion, tasks);
        observer.pushed(topic, insertion, creations, results);
        return results;
    }

    int topicId(StoreTransaction transaction, String topic) throws SQLException {
        return topics.id(transaction, topic);
    }

    @SuppressWarnings("unchecked")
    <T> PayloadCodec<T> codec(TopicRef<T> topic) {
        PayloadCodec<?> codec = topicCodecs.get(topic.name());
        if (codec == null) {
            codec = typeCodecs.get(topic.payloadType());
        }
        if (codec == null && topic.payloadType() == String.class) {
            codec = PayloadCodec.string();
        }
        if (codec == null) {
            throw new IllegalArgumentException("No payload codec for " + topic.payloadType().getName() + " (topic " + topic + ")");
        }
        return (PayloadCodec<T>) codec;
    }
}
