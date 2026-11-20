package org.example.task2.memory;

import org.example.task2.test.tck.AdmissionTck;
import org.example.task2.test.tck.StoreFixture;

class MemoryAdmissionTest extends AdmissionTck {

    @Override
    protected StoreFixture createFixture() {
        return new MemoryStoreFixture();
    }
}
