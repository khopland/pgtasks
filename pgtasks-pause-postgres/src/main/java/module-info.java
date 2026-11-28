/** Paused topics for pgtasks-pause on Postgres. */
module io.github.khopland.pgtasks.pause.postgres {
    requires transitive io.github.khopland.pgtasks.pause;
    requires transitive io.github.khopland.pgtasks.postgres;
    requires io.github.khopland.pgtasks.spi;

    exports io.github.khopland.pgtasks.pause.postgres;
    opens io.github.khopland.pgtasks.pause.postgres.db;

    provides io.github.khopland.pgtasks.spi.StoreCapability
        with io.github.khopland.pgtasks.pause.postgres.PostgresPauseCapability;
}
