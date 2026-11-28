/** Pausing and resuming topics across the cluster, built on the engine's extension hooks. */
module io.github.khopland.pgtasks.pause {
    requires transitive io.github.khopland.pgtasks.engine;
    requires org.jspecify;

    exports io.github.khopland.pgtasks.pause;

    provides io.github.khopland.pgtasks.spi.StoreCapability
        with io.github.khopland.pgtasks.pause.InMemoryPauseCapability;
}
