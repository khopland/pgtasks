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

    /**
     * Every topic whose name matches the pattern, such as {@code assessment-*}, including topics
     * created later at runtime: they run with this handler and these options. Overrides address
     * the pattern, as in {@code topics.assessment-*.concurrency}. See {@link TopicTemplate}.
     */
    <T> TaskRegistry topicsMatching(String pattern, Class<T> type, BatchHandler<T> handler, Consumer<TopicOptions> options);

    default <T> TaskRegistry topicsMatching(String pattern, Class<T> type, BatchHandler<T> handler) {
        return topicsMatching(pattern, type, handler, options -> { });
    }

    /** How payloads of this type are stored, for this module's topics and pushes. */
    <T> TaskRegistry codec(Class<T> type, PayloadCodec<T> codec);

    /**
     * Turns an application event into a task, so the publisher needs no dependency on the topic.
     * The push happens when the event is published, inside the publisher's transaction, so the
     * event's changes and the task commit or roll back together; an event published outside a
     * transaction pushes on its own. With Spring, the starter routes every application event.
     *
     * @param route the push for an event, or null to push nothing for it
     */
    <E> TaskRegistry onEvent(Class<E> event, java.util.function.Function<? super E, org.example.task2.Push<?>> route);

    /**
     * Pushes a task to one of this module's topics on a schedule, the same identifier and payload
     * each time. Every node that registers the topic records its schedules when it starts, and the
     * housekeeping leader pushes them when due, once each across the cluster. A run missed while
     * no node was up is made up once, not once per missed time. Each push supersedes the last one
     * if that is still waiting; see {@link Recurring#insertion()}.
     *
     * @param name unique within the topic
     */
    default <T> TaskRegistry recurring(TopicRef<T> topic, String name, Cron cron, String identifier, T payload) {
        return recurring(topic, name, cron, identifier, payload, org.example.task2.Insertion.SUPERSEDE);
    }

    <T> TaskRegistry recurring(TopicRef<T> topic, String name, Cron cron, String identifier, T payload, org.example.task2.Insertion insertion);
}
