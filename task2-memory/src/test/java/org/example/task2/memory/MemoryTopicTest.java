package org.example.task2.memory;

import org.example.task2.test.tck.StoreFixture;
import org.example.task2.test.tck.TopicTck;

class MemoryTopicTest extends TopicTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
