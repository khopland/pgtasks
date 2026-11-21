package io.github.khopland.pgtasks.memory;

import io.github.khopland.pgtasks.test.tck.StoreFixture;
import io.github.khopland.pgtasks.test.tck.ScheduleTck;

class MemoryScheduleTest extends ScheduleTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
