package io.github.khopland.task2.memory;

import io.github.khopland.task2.test.tck.PollOrderingTck;
import io.github.khopland.task2.test.tck.StoreFixture;

class MemoryPollOrderingTest extends PollOrderingTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
