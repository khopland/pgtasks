package io.github.khopland.task2.memory;

import io.github.khopland.task2.test.tck.StoreFixture;
import io.github.khopland.task2.test.tck.TopicTck;

class MemoryTopicTest extends TopicTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
