/** What task stores, notifiers and transaction bindings implement. Applications don't need it. */
module io.github.khopland.pgtasks.spi {
    requires transitive io.github.khopland.pgtasks;

    exports io.github.khopland.pgtasks.spi;
}
