package io.github.khopland.pgtasks.pause;

import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.github.khopland.pgtasks.spi.StoreTransaction;

/**
 * Paused topics kept in memory, next to the in-memory store's tasks, for local runs and tests. One
 * per store, shared by every engine on it. A change is undone with its transaction, or a savepoint
 * before it, like the store's own ({@link StoreTransaction#onRollback}).
 */
final class InMemoryPauseStore implements PauseStore {

    private final InstantSource clock;
    private final Map<String, Paused> paused = new TreeMap<>();

    InMemoryPauseStore(InstantSource clock) {
        this.clock = clock;
    }

    @Override
    public synchronized boolean pause(StoreTransaction transaction, String topic) {
        if (paused.putIfAbsent(topic, new Paused(topic, clock.instant())) != null) {
            return false;
        }
        transaction.onRollback(() -> remove(topic));
        return true;
    }

    @Override
    public synchronized boolean resume(StoreTransaction transaction, String topic) {
        Paused was = paused.remove(topic);
        if (was == null) {
            return false;
        }
        transaction.onRollback(() -> put(was));
        return true;
    }

    @Override
    public synchronized List<Paused> list(StoreTransaction transaction) {
        return List.copyOf(paused.values());
    }

    private synchronized void remove(String topic) {
        paused.remove(topic);
    }

    private synchronized void put(Paused was) {
        paused.put(was.topic(), was);
    }

    @Override
    public synchronized String toString() {
        return "InMemoryPauseStore" + paused.keySet();
    }
}
