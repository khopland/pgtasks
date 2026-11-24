package io.github.khopland.pgtasks.engine.extension;

import io.github.khopland.pgtasks.BatchHandler;
import io.github.khopland.pgtasks.TopicRef;

/**
 * Wraps the handler of every topic this node runs, such as to set up a context around each
 * batch. Decorators wrap in the order they were registered: the first is outermost. The
 * decorated handler is made when the topic starts running on this node.
 */
public interface HandlerDecorator {

    <T> BatchHandler<T> decorate(TopicRef<T> topic, BatchHandler<T> handler);
}
