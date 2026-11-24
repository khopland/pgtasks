/** An in-memory task store, for local runs and fast tests. */
module io.github.khopland.pgtasks.memory {
    requires transitive io.github.khopland.pgtasks.spi;

    exports io.github.khopland.pgtasks.memory;
}
