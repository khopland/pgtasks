package io.github.khopland.pgtasks.test.tck;

import org.junit.jupiter.api.AfterEach;

import org.jspecify.annotations.Nullable;

/**
 * Base of the store TCK. A store's test class extends each TCK class and creates its fixture;
 * the fixture is created lazily, so a subclass can set it up in its own {@code @BeforeEach}.
 */
public abstract class StoreTck {

    private @Nullable StoreFixture fixture;

    protected abstract StoreFixture createFixture() throws Exception;

    protected final StoreFixture fixture() {
        if (fixture == null) {
            try {
                fixture = createFixture();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException("Could not create the store fixture", e);
            }
        }
        return fixture;
    }

    @AfterEach
    void closeFixture() throws Exception {
        if (fixture != null) {
            fixture.close();
        }
    }
}
