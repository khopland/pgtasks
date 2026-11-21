package io.github.khopland.task2.memory;

import io.github.khopland.task2.test.tck.CompletionTck;
import io.github.khopland.task2.test.tck.StoreFixture;

class MemoryCompletionTest extends CompletionTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
