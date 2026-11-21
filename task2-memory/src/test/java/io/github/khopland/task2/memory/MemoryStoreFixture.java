package io.github.khopland.task2.memory;

import java.time.Duration;

import io.github.khopland.task2.spi.ManualClock;
import io.github.khopland.task2.spi.TaskStore;
import io.github.khopland.task2.test.tck.StoreFixture;

/** The TCK fixture for the in-memory store: time moves by advancing its clock. */
final class MemoryStoreFixture extends StoreFixture {

    private final ManualClock clock = ManualClock.startingNow();
    private final InMemoryTaskStore store = new InMemoryTaskStore(clock);

    @Override
    public TaskStore store() {
        return store;
    }

    @Override
    public void advance(Duration duration) {
        clock.advance(duration);
    }
}
