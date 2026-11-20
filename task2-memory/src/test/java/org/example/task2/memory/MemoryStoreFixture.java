package org.example.task2.memory;

import java.time.Duration;

import org.example.task2.spi.ManualClock;
import org.example.task2.spi.TaskStore;
import org.example.task2.test.tck.StoreFixture;

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
