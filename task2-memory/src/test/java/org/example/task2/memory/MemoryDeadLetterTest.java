package org.example.task2.memory;

import org.example.task2.test.tck.StoreFixture;
import org.example.task2.test.tck.DeadLetterTck;

class MemoryDeadLetterTest extends DeadLetterTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
