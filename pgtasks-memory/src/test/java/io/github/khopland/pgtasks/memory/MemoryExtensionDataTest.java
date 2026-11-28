package io.github.khopland.pgtasks.memory;

import io.github.khopland.pgtasks.test.tck.ExtensionDataTck;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

class MemoryExtensionDataTest extends ExtensionDataTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
