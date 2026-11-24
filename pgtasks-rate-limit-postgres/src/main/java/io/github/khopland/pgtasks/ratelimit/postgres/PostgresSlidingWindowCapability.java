package io.github.khopland.pgtasks.ratelimit.postgres;

import java.util.Optional;

import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.ratelimit.SlidingWindowStore;
import io.github.khopland.pgtasks.spi.StoreCapability;
import io.github.khopland.pgtasks.spi.TaskStore;

/** Gives {@code ClusterRateLimit} a shared counter when the engine runs on Postgres. */
public final class PostgresSlidingWindowCapability implements StoreCapability {

    /** Created by {@link java.util.ServiceLoader}, not by applications. */
    public PostgresSlidingWindowCapability() {
    }

    @Override
    public <C> Optional<C> create(Class<C> type, TaskStore store) {
        if (type != SlidingWindowStore.class || !(store instanceof PostgresTaskStore postgres)) {
            return Optional.empty();
        }
        return Optional.of(type.cast(new PostgresSlidingWindowStore(postgres)));
    }
}
