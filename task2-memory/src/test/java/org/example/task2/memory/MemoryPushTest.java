package org.example.task2.memory;

import org.example.task2.test.tck.PushTck;
import org.example.task2.test.tck.StoreFixture;

class MemoryPushTest extends PushTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
