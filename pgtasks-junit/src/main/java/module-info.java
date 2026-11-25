/** A JUnit 5 extension: an engine per test on the in-memory store, with a manual clock. */
module io.github.khopland.pgtasks.junit {
    requires transitive io.github.khopland.pgtasks.engine;
    requires transitive io.github.khopland.pgtasks.memory;
    requires transitive org.junit.jupiter.api;

    exports io.github.khopland.pgtasks.junit;
}
