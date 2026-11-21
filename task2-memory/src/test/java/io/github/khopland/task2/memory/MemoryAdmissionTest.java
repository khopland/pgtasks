package io.github.khopland.task2.memory;

import io.github.khopland.task2.test.tck.AdmissionTck;
import io.github.khopland.task2.test.tck.StoreFixture;

class MemoryAdmissionTest extends AdmissionTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
