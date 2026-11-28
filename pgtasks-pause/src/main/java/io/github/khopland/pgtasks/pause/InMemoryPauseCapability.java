package io.github.khopland.pgtasks.pause;

import java.util.Optional;

import io.github.khopland.pgtasks.spi.StoreCapability;
import io.github.khopland.pgtasks.spi.TaskStore;

/**
 * Gives {@link Pause} its paused topics when the engine runs on the in-memory store. One
 * {@link InMemoryPauseStore} per store ({@link TaskStore#shared}), so engines that share a store,
 * as a {@code TaskTester} with several nodes does, see the same pauses.
 */
public final class InMemoryPauseCapability implements StoreCapability {

    /** The store is recognised by its class name, so this module doesn't depend on pgtasks-memory. */
    private static final String IN_MEMORY_STORE = "io.github.khopland.pgtasks.memory.InMemoryTaskStore";

    /** Created by {@link java.util.ServiceLoader}, not by applications. */
    public InMemoryPauseCapability() {
    }

    @Override
    public <C> Optional<C> create(Class<C> type, TaskStore store) {
        if (type != PauseStore.class || !store.getClass().getName().equals(IN_MEMORY_STORE)) {
            return Optional.empty();
        }
        return Optional.of(type.cast(store.shared(InMemoryPauseStore.class, () -> new InMemoryPauseStore(store.clock()))));
    }
}
