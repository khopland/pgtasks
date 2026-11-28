package io.github.khopland.pgtasks.recurring;

import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.recurring.contract.ScheduleStoreContract;
import io.github.khopland.pgtasks.spi.ManualClock;
import io.github.khopland.pgtasks.spi.StoreCapability;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryScheduleStoreTest extends ScheduleStoreContract {

    private final InMemoryTaskStore store = new InMemoryTaskStore(ManualClock.startingNow());
    private final ScheduleStore schedules = new InMemoryScheduleCapability().create(ScheduleStore.class, store).orElseThrow();

    @Override
    protected TaskStore taskStore() {
        return store;
    }

    @Override
    protected ScheduleStore schedules() {
        return schedules;
    }

    @Test
    void engines_on_one_store_share_its_schedules_and_other_stores_get_none() {
        StoreCapability capability = new InMemoryScheduleCapability();
        assertThat(capability.create(ScheduleStore.class, store)).containsSame(schedules);
        assertThat(capability.create(ScheduleStore.class, new InMemoryTaskStore())).get().isNotSameAs(schedules);
        assertThat(capability.create(String.class, store)).isEmpty();
    }
}
