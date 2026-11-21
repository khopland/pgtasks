package io.github.khopland.pgtasks.memory;

import io.github.khopland.pgtasks.test.tck.CompletionTck;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

class MemoryCompletionTest extends CompletionTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
