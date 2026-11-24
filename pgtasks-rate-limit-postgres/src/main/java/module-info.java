/** Shared sliding-window counters for pgtasks-rate-limit on Postgres. */
module io.github.khopland.pgtasks.ratelimit.postgres {
    requires transitive io.github.khopland.pgtasks.ratelimit;
    requires io.github.khopland.pgtasks.postgres;
    requires io.github.khopland.pgtasks.spi;

    exports io.github.khopland.pgtasks.ratelimit.postgres;

    provides io.github.khopland.pgtasks.spi.StoreCapability
        with io.github.khopland.pgtasks.ratelimit.postgres.PostgresSlidingWindowCapability;
}
