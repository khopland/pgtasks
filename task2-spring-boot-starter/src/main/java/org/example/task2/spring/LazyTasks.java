package org.example.task2.spring;

import java.sql.Connection;
import java.util.List;
import java.util.function.Supplier;

import org.example.task2.Creation;
import org.example.task2.Insertion;
import org.example.task2.Pushed;
import org.example.task2.TaskWriter;
import org.example.task2.Tasks;
import org.example.task2.TopicRef;

/**
 * The {@code Tasks} bean. It finds the engine on first use instead of when it is injected, so a
 * service that pushes can be used by a handler: building the engine creates the configurers,
 * which create the handler and the service, and the service gets this bean without the engine
 * being built a second time.
 */
final class LazyTasks implements Tasks {

    private final Supplier<Tasks> tasks;

    LazyTasks(Supplier<Tasks> tasks) {
        this.tasks = tasks;
    }

    @Override
    public <T> List<Pushed> push(TopicRef<T> topic, Insertion insertion, List<Creation<T>> creations) {
        return tasks.get().push(topic, insertion, creations);
    }

    @Override
    public TaskWriter on(Connection connection) {
        return tasks.get().on(connection);
    }

    @Override
    public TaskWriter independently() {
        return tasks.get().independently();
    }

    @Override
    public String toString() {
        return "Tasks (resolved on first use)";
    }
}
