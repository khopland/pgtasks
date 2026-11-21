package org.example.task2.spring;

import java.time.Duration;

import org.example.task2.engine.TaskEngine;
import org.springframework.context.SmartLifecycle;

/**
 * Starts the engine once the context is refreshed and drains it when the context closes, before
 * the DataSource and the beans its handlers use are destroyed.
 */
public final class TaskEngineLifecycle implements SmartLifecycle {

    private final TaskEngine engine;
    private final Duration shutdownTimeout;
    private volatile boolean running;

    public TaskEngineLifecycle(TaskEngine engine, Duration shutdownTimeout) {
        this.engine = engine;
        this.shutdownTimeout = shutdownTimeout;
    }

    @Override
    public void start() {
        try {
            engine.start();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Could not start the task engine", e);
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
