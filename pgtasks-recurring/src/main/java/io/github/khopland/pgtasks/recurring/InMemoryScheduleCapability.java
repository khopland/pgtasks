package io.github.khopland.pgtasks.recurring;

import java.util.Optional;

import io.github.khopland.pgtasks.spi.StoreCapability;
import io.github.khopland.pgtasks.spi.TaskStore;

/**
 * Gives {@link Recurring} its schedules when the engine runs on the in-memory store. One
 * {@link InMemoryScheduleStore} per store ({@link TaskStore#shared}), so engines that share a
 * store, as a {@code TaskTester} with several nodes does, share its schedules and fire each run once.
 */
public final class InMemoryScheduleCapability implements StoreCapability {

    /** The store is recognised by its class name, so this module doesn't depend on pgtasks-memory. */
    private static final String IN_MEMORY_STORE = "io.github.khopland.pgtasks.memory.InMemoryTaskStore";

    /** Created by {@link java.util.ServiceLoader}, not by applications. */
    public InMemoryScheduleCapability() {
    }

    @Override
    public <C> Optional<C> create(Class<C> type, TaskStore store) {
        if (type != ScheduleStore.class || !store.getClass().getName().equals(IN_MEMORY_STORE)) {
            return Optional.empty();
        }
        return Optional.of(type.cast(store.shared(InMemoryScheduleStore.class, () -> new InMemoryScheduleStore(store.clock()))));
    }
}
