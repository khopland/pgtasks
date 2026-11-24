/** Metrics and tracing through Micrometer. */
module io.github.khopland.pgtasks.micrometer {
    requires transitive io.github.khopland.pgtasks.engine;
    requires transitive micrometer.core;
    requires transitive micrometer.observation;
    requires org.slf4j;

    exports io.github.khopland.pgtasks.micrometer;
}
