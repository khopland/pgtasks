package io.github.khopland.pgtasks.engine;

import java.util.Optional;

import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.StoreCapability;
import io.github.khopland.pgtasks.spi.TaskStore;

/** A capability for the in-memory store only, registered for the engine's tests. */
public final class TestCapabilities implements StoreCapability {

    /** Created by the engine's test service loader. */
    public TestCapabilities() {
    }

    /** What an extension asks for: the store's name, to show which store it got. */
    public interface StoreName {
        String name();
    }

    @Override
    public <C> Optional<C> create(Class<C> type, TaskStore store) {
        if (type == StoreName.class && store instanceof InMemoryTaskStore) {
            // A new one on each call, so a test can tell whether the engine made it once.
            StoreName name = new StoreName() {
                @Override
                public String name() {
                    return "memory";
                }
            };
            return Optional.of(type.cast(name));
        }
        return Optional.empty();
    }
}
