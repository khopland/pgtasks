package io.github.khopland.pgtasks.memory;

import io.github.khopland.pgtasks.test.tck.StoreFixture;
import io.github.khopland.pgtasks.test.tck.DeadLetterTck;

class MemoryDeadLetterTest extends DeadLetterTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
