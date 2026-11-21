package io.github.khopland.pgtasks.engine;

import java.sql.SQLException;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

/** Topic names to ids. Topics are never created on push; a misspelt name fails loudly. */
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
            int created = store.createTopic(transaction, name);
            ids.put(name, created);
            return created;
        }
        if (found.isEmpty()) {
            throw new IllegalArgumentException("Unknown topic '" + name + "'. Topics are created when the engine that "
                + "processes them starts, or explicitly with tasks.create_topic(); a push never creates one.");
        }
        ids.put(name, found.getAsInt());
        return found.getAsInt();
    }
}
