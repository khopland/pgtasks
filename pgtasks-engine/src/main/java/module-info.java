/**
 * The engine: builder, configurers, retries, housekeeping and {@code TaskTester}, and the
 * extension hooks. Finds its store through {@link io.github.khopland.pgtasks.spi.StoreProvider}
 * services, such as pgtasks-postgres.
 */
module io.github.khopland.pgtasks.engine {
    requires transitive io.github.khopland.pgtasks;
    requires transitive io.github.khopland.pgtasks.spi;
    requires org.slf4j;

    exports io.github.khopland.pgtasks.engine;
    exports io.github.khopland.pgtasks.engine.extension;

    uses io.github.khopland.pgtasks.spi.StoreProvider;
    uses io.github.khopland.pgtasks.spi.StoreCapability;
}
