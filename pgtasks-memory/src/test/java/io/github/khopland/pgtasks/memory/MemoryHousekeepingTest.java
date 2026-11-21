package io.github.khopland.pgtasks.memory;

import io.github.khopland.pgtasks.test.tck.HousekeepingTck;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

class MemoryHousekeepingTest extends HousekeepingTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
