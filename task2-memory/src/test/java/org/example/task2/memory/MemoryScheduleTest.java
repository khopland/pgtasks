package org.example.task2.memory;

import org.example.task2.test.tck.StoreFixture;
import org.example.task2.test.tck.ScheduleTck;

class MemoryScheduleTest extends ScheduleTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
