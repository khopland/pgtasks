/** A per-node rate limit for task claims, built on the engine's extension hooks. */
module io.github.khopland.pgtasks.ratelimit {
    requires transitive io.github.khopland.pgtasks.engine;
    requires org.jspecify;

    exports io.github.khopland.pgtasks.ratelimit;
}
