package io.github.khopland.pgtasks.pause.postgres;

import java.util.Optional;

import io.github.khopland.pgtasks.pause.PauseStore;
import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.spi.StoreCapability;
import io.github.khopland.pgtasks.spi.TaskStore;

/** Gives {@code Pause} its paused topics when the engine runs on Postgres. */
public final class PostgresPauseCapability implements StoreCapability {

    /** Created by {@link java.util.ServiceLoader}, not by applications. */
    public PostgresPauseCapability() {
    }

    @Override
    public <C> Optional<C> create(Class<C> type, TaskStore store) {
        if (type != PauseStore.class || !(store instanceof PostgresTaskStore postgres)) {
            return Optional.empty();
        }
        return Optional.of(type.cast(new PostgresPauseStore(postgres)));
    }
}
