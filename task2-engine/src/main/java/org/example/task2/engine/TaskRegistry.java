package org.example.task2.engine;

import java.util.List;
import java.util.function.Consumer;

import org.example.task2.BatchHandler;
import org.example.task2.PayloadCodec;
import org.example.task2.TopicRef;

/** What a {@link TaskConfigurer} registers its topics with. */
public interface TaskRegistry {

    /** Defaults for the topics this configurer registers, whichever order they are called in. */
    TaskRegistry defaults(Consumer<TopicOptions> defaults);

    <T> TaskRegistry topic(TopicRef<T> topic, BatchHandler<T> handler, Consumer<TopicOptions> options);

    default <T> TaskRegistry topic(TopicRef<T> topic, BatchHandler<T> handler) {
        return topic(topic, handler, options -> { });
    }

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

    /** How payloads of this type are stored, for this module's topics and pushes. */
    <T> TaskRegistry codec(Class<T> type, PayloadCodec<T> codec);
}
