package org.example.task2.engine;

import org.example.task2.PayloadCodec;

/**
 * Codecs for payload types nobody registered one for, such as JSON for every record: asked
 * last, after a topic's own codec, a template's and those given with
 * {@link TaskEngineBuilder#codec}. The Spring starter gives one that uses the application's
 * Jackson mapper.
 */
@FunctionalInterface
public interface PayloadCodecs {

    /** A codec for payloads of this type, or null if this source has none. */
    PayloadCodec<?> codecFor(Class<?> type);
}
