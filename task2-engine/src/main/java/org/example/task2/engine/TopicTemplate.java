package org.example.task2.engine;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import org.example.task2.BatchHandler;
import org.example.task2.BatchResource;
import org.example.task2.PayloadCodec;
import org.example.task2.TopicRef;

/**
 * Topics this node processes by name pattern, such as {@code assessment-*}: every registered topic
 * whose name matches runs with this handler and these settings, including topics created at
 * runtime, by any node or in SQL. A star matches any run of characters, none included.
 *
 * <p>A template never creates or destroys topics. A topic that matches both a template and a topic
 * registered by name runs as the named one.
 */
record TopicTemplate<T>(
    String pattern,
    Class<T> payloadType,
    BatchHandler<T> handler,
    PayloadCodec<T> codec,
    TopicSettings settings,
    List<BatchResource<?>> resources
) {

    private static final Pattern VALID = Pattern.compile("[a-z0-9*][a-z0-9._*-]{0,199}");

    public TopicTemplate {
        Objects.requireNonNull(pattern, "pattern");
        Objects.requireNonNull(payloadType, "payloadType");
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(codec, "codec");
        Objects.requireNonNull(settings, "settings");
        if (!VALID.matcher(pattern).matches() || !pattern.contains("*")) {
            throw new IllegalArgumentException("Invalid topic pattern '" + pattern + "': use [a-z0-9._-] and at least one *, like assessment-*");
        }
        resources = List.copyOf(resources);
    }

    public static <T> TopicTemplate<T> of(String pattern, Class<T> payloadType, PayloadCodec<T> codec, BatchHandler<T> handler) {
        return new TopicTemplate<>(pattern, payloadType, handler, codec, TopicSettings.defaults(), List.of());
    }

    public static TopicTemplate<String> of(String pattern, BatchHandler<String> handler) {
        return of(pattern, String.class, PayloadCodec.string(), handler);
    }

    public TopicTemplate<T> with(TopicSettings settings) {
        return new TopicTemplate<>(pattern, payloadType, handler, codec, settings, resources);
    }

    public boolean matches(String topic) {
        return regex().matcher(topic).matches();
    }

    /** The topic as this node runs it. */
    Topic<T> instantiate(String topic) {
        return new Topic<>(TopicRef.of(topic, payloadType), handler, codec, settings, resources);
    }

    private Pattern regex() {
        StringBuilder regex = new StringBuilder();
        for (String part : pattern.split("\\*", -1)) {
            if (!regex.isEmpty()) {
                regex.append(".*");
            }
            regex.append(Pattern.quote(part));
        }
        return Pattern.compile(regex.toString());
    }

    @Override
    public String toString() {
        return pattern;
    }
}
