package org.example.task2;

/**
 * One task inside {@link Batch#decide}: read it, and stage downstream work and junctions that are
 * committed together with the batch's completion, but only if this task's final decision
 * continues.
 */
public interface Work<T> {

    Task<T> task();

    default long id() {
        return task().id();
    }

    default String identifier() {
        return task().identifier();
    }

    default T payload() {
        return task().payload();
    }

    default int attempt() {
        return task().attempt();
    }

    default String idempotencyKey() {
        return task().idempotencyKey();
    }

    default <C> void push(TopicRef<C> topic, String identifier, C payload) {
        push(topic, Insertion.APPEND, Creation.of(identifier, payload));
    }

    default <C> void push(TopicRef<C> topic, String identifier, C payload, Insertion insertion) {
        push(topic, insertion, Creation.of(identifier, payload));
    }

    <C> void push(TopicRef<C> topic, Insertion insertion, Creation<C> creation);

    /**
     * Stages a junction: if this task continues, its identifier and the given ones form a group in
     * the target topic, whose suspended tasks become ready once every identifier has a task there.
     */
    void junction(TopicRef<?> topic, Juncture juncture, java.util.Set<String> identifiers);
}
