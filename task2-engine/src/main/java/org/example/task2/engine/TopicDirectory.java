package org.example.task2.engine;

import java.sql.SQLException;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;

import org.example.task2.spi.StoreTransaction;
import org.example.task2.spi.TaskStore;

/** Topic names to ids. Topics are never created on push; a misspelt name fails loudly. */
final class TopicDirectory {

    private final TaskStore store;
    private final Map<String, Integer> ids = new ConcurrentHashMap<>();

    TopicDirectory(TaskStore store) {
        this.store = store;
    }

    void register(String name, int id) {
        ids.put(name, id);
    }

    int id(StoreTransaction transaction, String name) throws SQLException {
        Integer id = ids.get(name);
        if (id != null) {
            return id;
        }
        OptionalInt found = store.findTopic(transaction, name);
        if (found.isEmpty()) {
            throw new IllegalArgumentException("Unknown topic '" + name + "'. Topics are created when the engine that "
                + "processes them starts, or explicitly with tasks.create_topic(); a push never creates one.");
        }
        ids.put(name, found.getAsInt());
        return found.getAsInt();
    }
}
