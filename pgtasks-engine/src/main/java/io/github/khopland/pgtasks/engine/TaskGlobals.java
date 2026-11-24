package io.github.khopland.pgtasks.engine;

/** Settings shared by every module: housekeeping, node settings. At most one per engine. */
@FunctionalInterface
public interface TaskGlobals {

    void configure(GlobalOptions globals);
}
