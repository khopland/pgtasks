package io.github.khopland.pgtasks.spring;

import java.time.Duration;

import io.github.khopland.pgtasks.engine.TaskEngine;

import org.springframework.context.SmartLifecycle;

/**
 * Starts the engine once the context is refreshed and drains it when the context closes, before
 * the DataSource and the beans its handlers use are destroyed. With {@code pgtasks.enabled=false}
 * it only prepares the engine: the schema and the configured topics, so pushes work, and no
 * processing, housekeeping or notification listening on this node.
 */
public final class TaskEngineLifecycle implements SmartLifecycle {

    private final TaskEngine engine;
    private final Duration shutdownTimeout;
    private final boolean processing;
    private volatile boolean running;

    public TaskEngineLifecycle(TaskEngine engine, Duration shutdownTimeout) {
        this(engine, shutdownTimeout, true);
    }

    /** @param processing false for a node that only pushes: the engine is prepared, never started */
    public TaskEngineLifecycle(TaskEngine engine, Duration shutdownTimeout, boolean processing) {
        this.engine = engine;
        this.shutdownTimeout = shutdownTimeout;
        this.processing = processing;
    }

    @Override
    public void start() {
        if (processing) {
            engine.start();
        } else {
            engine.prepare();
        }
        running = true;
    }

    @Override
    public void stop() {
        engine.stop(shutdownTimeout);
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
