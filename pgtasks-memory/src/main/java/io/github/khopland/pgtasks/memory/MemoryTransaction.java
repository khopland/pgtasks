package io.github.khopland.pgtasks.memory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.sql.DataSource;

import io.github.khopland.pgtasks.spi.StoreTransaction;

import org.jspecify.annotations.Nullable;

/**
 * A transaction in the in-memory store. It holds the store's lock from begin to commit or
 * rollback, so transactions never interleave across threads, and records an undo entry for every
 * change.
 *
 * <p>With a data source, the connection from {@link #connection()} opens a JDBC transaction on its
 * first call, for batch resources' JDBC effects. It commits just before the store's own changes, and a failed
 * commit rolls those back; it is rolled back with them, savepoints included.
 */
final class MemoryTransaction implements StoreTransaction {

    @SuppressWarnings("ReferenceEquality") // a proxy equals only itself
    private static final Connection NO_CONNECTION = (Connection) Proxy.newProxyInstance(
        Connection.class.getClassLoader(),
        new Class<?>[] {Connection.class},
        (proxy, method, arguments) -> switch (method.getName()) {
            case "toString" -> "no connection (in-memory store)";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == arguments[0];
            default -> throw new SQLFeatureNotSupportedException("The in-memory store has no database connection. "
                + "A batch resource with JDBC effects needs the Postgres store, or an InMemoryTaskStore given a DataSource.");
        }
    );

    private final InMemoryTaskStore store;
    private final @Nullable DataSource dataSource;
    /** The JDBC transaction, once something has used the connection. */
    private @Nullable Connection connection;
    /** What {@link #connection()} hands out: opens {@link #connection} when first called. */
    private @Nullable Connection lazy;
    private final Deque<Runnable> undo = new ArrayDeque<>();
    private final Set<Integer> readyTopics = new LinkedHashSet<>();
    /** Sent on commit; like NOTIFY, a message sent twice in one transaction is delivered once. */
    private final Set<Message> messages = new LinkedHashSet<>();

    record Message(String channel, String payload) {
    }
    private boolean done;

    MemoryTransaction(InMemoryTaskStore store, @Nullable DataSource dataSource) {
        this.store = store;
        this.dataSource = dataSource;
    }

    <K, V> void put(Map<K, V> table, K key, V value) {
        check();
        boolean present = table.containsKey(key);
        V previous = table.put(key, value);
        undo.push(() -> {
            if (present) {
                table.put(key, previous);
            } else {
                table.remove(key);
            }
        });
    }

    <K, V> void remove(Map<K, V> table, K key) {
        check();
        if (table.containsKey(key)) {
            V previous = table.remove(key);
            undo.push(() -> table.put(key, previous));
        }
    }

    /** Like the queue's NOTIFY triggers: a row became ready, so wake the topic after commit. */
    void ready(int topicId) {
        readyTopics.add(topicId);
    }

    /** Like NOTIFY: tell every engine listening to the channel after commit. */
    void send(String channel, String payload) {
        messages.add(new Message(channel, payload));
    }

    private void check() {
        if (done) {
            throw new IllegalStateException("The transaction has ended");
        }
    }

    @Override
    public void commit() throws SQLException {
        check();
        Connection jdbc = connection;
        if (jdbc != null) {
            try {
                jdbc.commit();
            } catch (SQLException | RuntimeException e) {
                try {
                    rollback();
                } catch (SQLException | RuntimeException rollback) {
                    e.addSuppressed(rollback);
                }
                throw e;
            }
            connection = null;
            closeQuietly(jdbc);
        }
        done = true;
        undo.clear();
        store.unlock();
        store.notifyReady(readyTopics);
        store.deliver(messages);
    }

    @Override
    public void rollback() throws SQLException {
        check();
        done = true;
        Connection jdbc = connection;
        connection = null;
        try {
            if (jdbc != null) {
                try {
                    jdbc.rollback();
                } finally {
                    closeQuietly(jdbc);
                }
            }
        } finally {
            while (!undo.isEmpty()) {
                undo.pop().run();
            }
            store.unlock();
        }
    }

    /** Undoes the changes recorded since, newest first; wake-ups for them are dropped too. */
    @Override
    public Savepoint savepoint() throws SQLException {
        check();
        // Opened now if not yet, so JDBC effects after this point roll back with it.
        java.sql.@Nullable Savepoint jdbc = dataSource != null ? open().setSavepoint() : null;
        int changes = undo.size();
        Set<Integer> ready = Set.copyOf(readyTopics);
        Set<Message> sent = Set.copyOf(messages);
        return new Savepoint() {
            @Override
            public void rollback() throws SQLException {
                check();
                while (undo.size() > changes) {
                    undo.pop().run();
                }
                readyTopics.retainAll(ready);
                messages.retainAll(sent);
                if (jdbc != null) {
                    Connection open = open();
                    open.rollback(jdbc);
                    open.releaseSavepoint(jdbc);
                }
            }

            @Override
            public void release() throws SQLException {
                if (jdbc != null) {
                    open().releaseSavepoint(jdbc);
                }
            }
        };
    }

    /**
     * The completion hands this to every batch resource, whether it writes SQL or not, so with a
     * data source it is a stand-in that opens the JDBC transaction on its first call: a batch
     * whose resources never use it never takes a connection.
     */
    @Override
    public Connection connection() {
        if (dataSource == null) {
            return NO_CONNECTION;
        }
        Connection handed = lazy;
        if (handed == null) {
            handed = opensOnFirstUse();
            lazy = handed;
        }
        return handed;
    }

    @SuppressWarnings("ReferenceEquality") // a proxy equals only itself
    private Connection opensOnFirstUse() {
        return (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, arguments) -> switch (method.getName()) {
                case "toString" -> "connection for the in-memory store's JDBC effects";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == arguments[0];
                default -> {
                    try {
                        yield method.invoke(open(), arguments);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }
            }
        );
    }

    private Connection open() throws SQLException {
        check();
        Connection jdbc = connection;
        if (jdbc == null) {
            jdbc = Objects.requireNonNull(dataSource).getConnection();
            try {
                jdbc.setAutoCommit(false);
            } catch (SQLException | RuntimeException e) {
                closeQuietly(jdbc);
                throw e;
            }
            connection = jdbc;
        }
        return jdbc;
    }

    private static void closeQuietly(Connection jdbc) {
        try {
            jdbc.close();
        } catch (SQLException ignored) {
            // Committed or rolled back already; a pool reclaims what close couldn't.
        }
    }

    @Override
    public void close() throws SQLException {
        if (!done) {
            rollback();
        }
    }
}
