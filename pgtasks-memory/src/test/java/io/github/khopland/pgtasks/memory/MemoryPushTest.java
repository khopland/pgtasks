package io.github.khopland.pgtasks.memory;

import io.github.khopland.pgtasks.test.tck.PushTck;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

class MemoryPushTest extends PushTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
