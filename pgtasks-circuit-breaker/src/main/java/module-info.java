/** A per-node circuit breaker for task claims, built on the engine's extension hooks. */
module io.github.khopland.pgtasks.circuitbreaker {
    requires transitive io.github.khopland.pgtasks.engine;
    requires org.jspecify;
    requires org.slf4j;

    exports io.github.khopland.pgtasks.circuitbreaker;
}
