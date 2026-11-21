package io.github.khopland.pgtasks.spring;

import java.sql.Connection;
import java.util.List;
import java.util.function.Supplier;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.TaskWriter;
import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.TopicRef;

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
