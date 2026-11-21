package io.github.khopland.task2.memory;

import io.github.khopland.task2.test.tck.PushTck;
import io.github.khopland.task2.test.tck.StoreFixture;

class MemoryPushTest extends PushTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
