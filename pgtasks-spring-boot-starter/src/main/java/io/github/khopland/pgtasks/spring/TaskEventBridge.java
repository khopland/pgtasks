package io.github.khopland.pgtasks.spring;

import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskRegistry;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.PayloadApplicationEvent;

/**
 * Hands every application event to the engine's event routes ({@link TaskRegistry#onEvent}).
 * It listens synchronously, on the publisher's thread, so a route's push joins the publisher's
 * transaction: the event's changes and the task commit or roll back together. (Spring Modulith's
 * {@code @ApplicationModuleListener} runs after the commit, in a new transaction, which would
 * lose that.)
 */
public class TaskEventBridge implements ApplicationListener<ApplicationEvent> {

    private final TaskEngine engine;

    public TaskEventBridge(TaskEngine engine) {
        this.engine = engine;
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (engine.eventRoutes().isEmpty()) {
            return;
        }
        engine.publish(event instanceof PayloadApplicationEvent<?> payload ? payload.getPayload() : event);
    }
}
