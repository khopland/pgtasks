package io.github.khopland.pgtasks.memory;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.github.khopland.pgtasks.spi.StoreTransaction;

/**
 * A transaction in the in-memory store. It holds the store's lock from begin to commit or
 * rollback, so transactions never interleave across threads, and records an undo entry for every
 * change.
 */
final class MemoryTransaction implements StoreTransaction {

    private static final Connection NO_CONNECTION = (Connection) Proxy.newProxyInstance(
        Connection.class.getClassLoader(),
        new Class<?>[] {Connection.class},
        (proxy, method, arguments) -> switch (method.getName()) {
            case "toString" -> "no connection (in-memory store)";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == arguments[0];
            default -> throw new SQLFeatureNotSupportedException("The in-memory store has no database connection. "
                + "A batch resource with JDBC effects needs the Postgres store.");
        }
    );

    private final InMemoryTaskStore store;
    private final Deque<Runnable> undo = new ArrayDeque<>();
    private final Set<Integer> readyTopics = new LinkedHashSet<>();
    private final Set<String> changedLimits = new LinkedHashSet<>();
    private final Set<String> changedTopics = new LinkedHashSet<>();
    private boolean done;

    MemoryTransaction(InMemoryTaskStore store) {
        this.store = store;
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

    /** Deletes the rows that match; a rollback puts them back where they were. */
    <T> void removeIf(List<T> table, java.util.function.Predicate<? super T> which) {
        check();
        List<T> before = List.copyOf(table);
        if (table.removeIf(which)) {
            undo.push(() -> {
                table.clear();
                table.addAll(before);
            });
        }
    }

    <T> void append(List<T> table, T row) {
        check();
        table.add(row);
        undo.push(() -> table.removeLast());
    }

    /** Like the queue's NOTIFY triggers: a row became ready, so wake the topic after commit. */
    void ready(int topicId) {
        readyTopics.add(topicId);
    }

    /** Like the topic NOTIFY triggers: tell every engine after commit. */
    void topicChanged(String name) {
        changedTopics.add(name);
    }

    /** Like the rate limit NOTIFY trigger: tell every engine after commit. */
    void limitChanged(String name) {
        changedLimits.add(name);
    }

    private void check() {
        if (done) {
            throw new IllegalStateException("The transaction has ended");
        }
    }

    @Override
    public void commit() {
        check();
        done = true;
        undo.clear();
        store.unlock();
        store.notifyReady(readyTopics);
        store.notifyTopics(changedTopics);
        store.notifyLimits(changedLimits);
    }

    @Override
    public void rollback() {
        check();
        done = true;
        while (!undo.isEmpty()) {
            undo.pop().run();
        }
        store.unlock();
    }

    @Override
    public Connection connection() {
        return NO_CONNECTION;
    }

    @Override
    public void close() {
        if (!done) {
            rollback();
        }
    }
}
