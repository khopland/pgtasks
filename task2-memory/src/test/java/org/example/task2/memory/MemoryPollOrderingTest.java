package org.example.task2.memory;

import org.example.task2.test.tck.PollOrderingTck;
import org.example.task2.test.tck.StoreFixture;

class MemoryPollOrderingTest extends PollOrderingTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
