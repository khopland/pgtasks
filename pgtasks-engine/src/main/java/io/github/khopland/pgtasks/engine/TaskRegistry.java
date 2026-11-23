package io.github.khopland.pgtasks.engine;

import java.util.List;
import java.util.function.Consumer;

import io.github.khopland.pgtasks.BatchHandler;
import io.github.khopland.pgtasks.PayloadCodec;
import io.github.khopland.pgtasks.TaskHandler;
import io.github.khopland.pgtasks.TopicRef;

/** What a {@link TaskConfigurer} registers its topics with. */
public interface TaskRegistry {

    /** Defaults for the topics this configurer registers, whichever order they are called in. */
    TaskRegistry defaults(Consumer<TopicOptions> defaults);

    <T> TaskRegistry topic(TopicRef<T> topic, BatchHandler<T> handler, Consumer<TopicOptions> options);

    default <T> TaskRegistry topic(TopicRef<T> topic, BatchHandler<T> handler) {
        return topic(topic, handler, options -> { });
    }

    /**
     * A topic whose tasks are handled one at a time: each succeeds when the handler returns and
     * fails when it throws. The shortest way to register a topic:
     * {@code tasks.eachTask(NOTIFY, task -> notifier.send(task.payload()))}.
     */
    default <T> TaskRegistry eachTask(TopicRef<T> topic, TaskHandler<T> handler) {
        return topic(topic, BatchHandler.each(handler));
    }

    default <T> TaskRegistry eachTask(TopicRef<T> topic, TaskHandler<T> handler, Consumer<TopicOptions> options) {
        return topic(topic, BatchHandler.each(handler), options);
    }

    /**
     * Topics this module pushes to but doesn't process, such as another module's. Each is created
     * when the engine starts or is prepared, so the pushes work whichever service is deployed
     * first, and the build fails if there is no codec for its payloads. Declaring them also says,
     * in one place, what this module depends on.
     */
    TaskRegistry pushesTo(TopicRef<?>... topics);

    default <T> TaskRegistry topic(String name, Class<T> type, BatchHandler<T> handler) {
        return topic(TopicRef.of(name, type), handler);
    }

    default <T> TaskRegistry topic(String name, Class<T> type, BatchHandler<T> handler, Consumer<TopicOptions> options) {
        return topic(TopicRef.of(name, type), handler, options);
    }

    /** Several topics with one handler and the same options. */
    default <T> TaskRegistry topics(List<String> names, Class<T> type, BatchHandler<T> handler, Consumer<TopicOptions> options) {
        names.forEach(name -> topic(TopicRef.of(name, type), handler, options));
        return this;
    }

    default <T> TaskRegistry topics(List<String> names, Class<T> type, BatchHandler<T> handler) {
        return topics(names, type, handler, options -> { });
    }

    /**
     * Several topics of one payload type with one handler and the same options, such as one per
     * year or per region, each registered as if on its own.
     */
    default <T> TaskRegistry topics(List<TopicRef<T>> topics, BatchHandler<T> handler, Consumer<TopicOptions> options) {
        topics.forEach(topic -> topic(topic, handler, options));
        return this;
    }

    default <T> TaskRegistry topics(List<TopicRef<T>> topics, BatchHandler<T> handler) {
        return topics(topics, handler, options -> { });
    }

    /** How payloads of this type are stored, for this module's topics and pushes. */
    <T> TaskRegistry codec(Class<T> type, PayloadCodec<T> codec);
}
