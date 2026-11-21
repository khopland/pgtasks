package io.github.khopland.task2.memory;

import io.github.khopland.task2.test.tck.JunctionTck;
import io.github.khopland.task2.test.tck.StoreFixture;

class MemoryJunctionTest extends JunctionTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
