package org.example.task2.memory;

import org.example.task2.test.tck.JunctionTck;
import org.example.task2.test.tck.StoreFixture;

class MemoryJunctionTest extends JunctionTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
