package io.github.khopland.pgtasks.engine;

import java.util.List;
import java.util.Objects;

import io.github.khopland.pgtasks.BatchHandler;
import io.github.khopland.pgtasks.BatchResource;
import io.github.khopland.pgtasks.PayloadCodec;
import io.github.khopland.pgtasks.TopicRef;

/**
 * A topic this node processes: its reference, handler, codec, settings and batch resources.
 * Resources review in the order given, and write in that order too.
 */
record Topic<T>(
    TopicRef<T> ref,
    BatchHandler<T> handler,
    PayloadCodec<T> codec,
    TopicSettings settings,
    List<BatchResource<?>> resources
) {

    public Topic {
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(codec, "codec");
        Objects.requireNonNull(settings, "settings");
        resources = List.copyOf(resources);
    }

    public static <T> Topic<T> of(TopicRef<T> ref, PayloadCodec<T> codec, BatchHandler<T> handler) {
        return new Topic<>(ref, handler, codec, TopicSettings.defaults(), List.of());
    }

    public static Topic<String> of(TopicRef<String> ref, BatchHandler<String> handler) {
        return of(ref, PayloadCodec.string(), handler);
    }

    public Topic<T> with(TopicSettings settings) {
        return new Topic<>(ref, handler, codec, settings, resources);
    }

    public Topic<T> withResources(BatchResource<?>... resources) {
        return new Topic<>(ref, handler, codec, settings, List.of(resources));
    }
}
