package io.github.khopland.pgtasks.memory;

import io.github.khopland.pgtasks.test.tck.PollOrderingTck;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

class MemoryPollOrderingTest extends PollOrderingTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
