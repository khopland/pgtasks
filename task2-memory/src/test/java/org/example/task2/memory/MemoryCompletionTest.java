package org.example.task2.memory;

import org.example.task2.test.tck.CompletionTck;
import org.example.task2.test.tck.StoreFixture;

class MemoryCompletionTest extends CompletionTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
