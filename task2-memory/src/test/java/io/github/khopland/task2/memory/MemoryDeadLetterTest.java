package io.github.khopland.task2.memory;

import io.github.khopland.task2.test.tck.StoreFixture;
import io.github.khopland.task2.test.tck.DeadLetterTck;

class MemoryDeadLetterTest extends DeadLetterTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
