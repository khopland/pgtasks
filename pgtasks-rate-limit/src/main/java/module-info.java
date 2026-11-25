/** Rate limits for task claims, per node and across the cluster, built on the engine's extension hooks. */
module io.github.khopland.pgtasks.ratelimit {
    requires transitive io.github.khopland.pgtasks.engine;
    requires org.jspecify;

    exports io.github.khopland.pgtasks.ratelimit;
}
