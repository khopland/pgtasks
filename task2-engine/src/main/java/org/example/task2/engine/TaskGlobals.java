package org.example.task2.engine;

/** Settings shared by every module: rate limits, housekeeping, node settings. At most one per engine. */
@FunctionalInterface
public interface TaskGlobals {

    void configure(GlobalOptions globals);
}
