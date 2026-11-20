package org.example.task2.engine;

import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.random.RandomGenerator;

import org.example.task2.spi.TaskStore;

/** What every worker on this node shares. */
record EngineCore(
    TaskStore store,
    Jdbc jdbc,
    DefaultTasks tasks,
    int nodeId,
    Semaphore inFlight,
    RandomGenerator random,
    Consumer<String> wake
) {
}
