package io.github.khopland.task2.engine;

import java.util.concurrent.Callable;

/**
 * Marks the threads that run a topic's handler, so a push from there can be told apart: one
 * that should have been staged with {@code task.push(...)}, to commit with the task's completion.
 */
final class HandlerThread {

    private static final ScopedValue<String> TOPIC = ScopedValue.newInstance();

    private HandlerThread() {
    }

    /** Runs the work as a handler of the topic. */
    static <V> V run(String topic, Callable<V> work) throws Exception {
        return ScopedValue.where(TOPIC, topic).call(work::call);
    }

    /** The topic whose handler this thread runs, or null. */
    static String topic() {
        return TOPIC.isBound() ? TOPIC.get() : null;
    }
}
