package io.github.khopland.pgtasks.memory;

import io.github.khopland.pgtasks.test.tck.AdmissionTck;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

class MemoryAdmissionTest extends AdmissionTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
