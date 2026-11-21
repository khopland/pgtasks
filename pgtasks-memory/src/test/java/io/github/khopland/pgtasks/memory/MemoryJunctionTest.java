package io.github.khopland.pgtasks.memory;

import io.github.khopland.pgtasks.test.tck.JunctionTck;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

class MemoryJunctionTest extends JunctionTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
