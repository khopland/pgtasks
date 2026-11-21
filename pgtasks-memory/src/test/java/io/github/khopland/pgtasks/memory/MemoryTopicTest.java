package io.github.khopland.pgtasks.memory;

import io.github.khopland.pgtasks.test.tck.StoreFixture;
import io.github.khopland.pgtasks.test.tck.TopicTck;

class MemoryTopicTest extends TopicTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
