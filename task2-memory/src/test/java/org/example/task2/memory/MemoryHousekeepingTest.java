package org.example.task2.memory;

import org.example.task2.test.tck.HousekeepingTck;
import org.example.task2.test.tck.StoreFixture;

class MemoryHousekeepingTest extends HousekeepingTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
