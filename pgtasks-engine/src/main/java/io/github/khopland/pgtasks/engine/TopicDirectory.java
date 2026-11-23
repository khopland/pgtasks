package io.github.khopland.pgtasks.engine;

import java.sql.SQLException;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

/**
 * Topic names to ids. Topics are created on push only with {@link TaskEngineBuilder#topicsCreatedOnPush};
 * otherwise a misspelt name fails loudly. Only ids of committed topics are cached: one created on
 * push is created in a short transaction of its own, so a rollback of the push's transaction can't
 * leave an id cached for a topic that doesn't exist.
 */
final class TopicDirectory {

    private final TaskStore store;
    private final Map<String, Integer> ids = new ConcurrentHashMap<>();
    private volatile boolean createOnPush;

    TopicDirectory(TaskStore store) {
        this.store = store;
    }

    /** See {@link TaskEngineBuilder#topicsCreatedOnPush}. */
    void createOnPush() {
        createOnPush = true;
    }

    /** Whether the topic's id is known here already, so looking it up costs nothing. */
    boolean known(String name) {
        return createOnPush || ids.containsKey(name);
    }

    /** Forgets the topics the store no longer has. */
    void retain(Set<String> names) {
        ids.keySet().retainAll(names);
    }

    /**
     * Forgets the cached id of a topic, if it is still the given one: a push to it failed as if the
     * topic were gone, say deleted by another node, so the next push looks it up again. A node that
     * only pushes has no heartbeat that would {@link #retain} the topics.
     */
    void forget(String name, int id) {
        ids.remove(name, id);
    }

    void register(String name, int id) {
        ids.put(name, id);
    }

    /** The name of a topic id seen before, or the id as text. */
    String name(int id) {
        return ids.entrySet().stream().filter(entry -> entry.getValue() == id).map(Map.Entry::getKey).findFirst().orElse("#" + id);
    }

    int id(StoreTransaction transaction, String name) throws SQLException {
        Integer id = ids.get(name);
        if (id != null) {
            return id;
        }
        OptionalInt found = store.findTopic(transaction, name);
        if (found.isEmpty() && createOnPush) {
            // Its own transaction, committed before the id is cached: created in the caller's, the
            // topic would vanish if that rolled back, and the cached id would fail every later push.
            // create_topic is idempotent, so a concurrent creation on another node is harmless.
            int created;
            try {
                created = EngineCore.transact(store, own -> store.createTopic(own, name));
            } catch (SQLException | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            ids.put(name, created);
            return created;
        }
        if (found.isEmpty()) {
            throw new IllegalArgumentException("Unknown topic '" + name + "'. Topics are created when the engine that "
                + "processes them starts, or explicitly with pgtasks.create_topic(); a push never creates one.");
        }
        ids.put(name, found.getAsInt());
        return found.getAsInt();
    }
}
