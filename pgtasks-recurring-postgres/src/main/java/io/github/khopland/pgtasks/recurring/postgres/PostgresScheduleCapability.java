package io.github.khopland.pgtasks.recurring.postgres;

import java.util.Optional;

import io.github.khopland.pgtasks.postgres.PostgresTaskStore;
import io.github.khopland.pgtasks.recurring.ScheduleStore;
import io.github.khopland.pgtasks.spi.StoreCapability;
import io.github.khopland.pgtasks.spi.TaskStore;

/** Gives {@code Recurring} its shared schedules when the engine runs on Postgres. */
public final class PostgresScheduleCapability implements StoreCapability {

    /** Created by {@link java.util.ServiceLoader}, not by applications. */
    public PostgresScheduleCapability() {
    }

    @Override
    public <C> Optional<C> create(Class<C> type, TaskStore store) {
        if (type != ScheduleStore.class || !(store instanceof PostgresTaskStore postgres)) {
            return Optional.empty();
        }
        return Optional.of(type.cast(new PostgresScheduleStore(postgres)));
    }
}
