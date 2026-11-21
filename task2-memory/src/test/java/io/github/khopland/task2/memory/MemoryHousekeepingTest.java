package io.github.khopland.task2.memory;

import io.github.khopland.task2.test.tck.HousekeepingTck;
import io.github.khopland.task2.test.tck.StoreFixture;

class MemoryHousekeepingTest extends HousekeepingTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
