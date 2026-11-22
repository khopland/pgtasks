package io.github.khopland.pgtasks.memory;

import io.github.khopland.pgtasks.test.tck.OperatorTck;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

class MemoryOperatorTest extends OperatorTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
