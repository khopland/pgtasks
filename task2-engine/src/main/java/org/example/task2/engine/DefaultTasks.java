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
import org.example.task2.spi.TaskStore;
import org.example.task2.spi.TransactionBinding;

/** Transaction-aware enqueue. Only {@link #independently()} ever commits. */
final class DefaultTasks implements Tasks {

    private final TaskStore store;
    private final TopicDirectory topics;
    private final Map<String, PayloadCodec<?>> topicCodecs;
    private final Map<Class<?>, PayloadCodec<?>> typeCodecs;
    private final TransactionBinding transactions;
    private final Jdbc jdbc;
    private final Consumer<String> wakeLocally;

    DefaultTasks(
        TaskStore store,
        TopicDirectory topics,
        Map<String, PayloadCodec<?>> topicCodecs,
        Map<Class<?>, PayloadCodec<?>> typeCodecs,
        TransactionBinding transactions,
        Jdbc jdbc,
        Consumer<String> wakeLocally
    ) {
        this.store = store;
        this.topics = topics;
        this.topicCodecs = Map.copyOf(topicCodecs);
        this.typeCodecs = Map.copyOf(typeCodecs);
        this.transactions = transactions;
        this.jdbc = jdbc;
        this.wakeLocally = wakeLocally;
    }

    @Override
    public <T> List<Pushed> push(TopicRef<T> topic, Insertion insertion, List<Creation<T>> creations) {
        Connection connection = transactions.current().orElseThrow(() -> new IllegalStateException(
            "tasks.push(...) joins the current transaction, but there is none. Run it inside a transaction, "
                + "use tasks.on(connection) to push on an explicit connection, or tasks.independently() to commit on its own."
        ));
        return pushOn(connection, topic, insertion, creations);
    }

    @Override
    public TaskWriter on(Connection connection) {
        Objects.requireNonNull(connection, "connection");
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
                    pushed = jdbc.transact(connection -> write(connection, topic, insertion, creations, null));
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
            if (connection.getAutoCommit()) {
                throw new IllegalStateException("The connection is in auto-commit mode, so the push would commit on its own. "
                    + "Start a transaction first, or use tasks.independently() if that is what you want.");
            }
            return write(connection, topic, insertion, creations, null);
        } catch (SQLException e) {
            throw new TaskStorageException("Could not push to " + topic, e);
        }
    }

    /**
     * Writes on the given connection without committing. {@code parents}, if given, holds the
     * task that staged each creation, for the lineage columns.
     */
    <T> List<Pushed> write(Connection connection, TopicRef<T> topic, Insertion insertion, List<Creation<T>> creations, List<? extends Task<?>> parents) throws SQLException {
        if (creations.isEmpty()) {
            return List.of();
        }
        PayloadCodec<T> codec = codec(topic);
        int topicId = topics.id(connection, topic.name());
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
        return store.push(connection, topicId, insertion, tasks);
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
