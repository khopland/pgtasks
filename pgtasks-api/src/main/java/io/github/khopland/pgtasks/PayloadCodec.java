package io.github.khopland.pgtasks;

import java.util.Objects;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

/** Turns payloads into the text stored in {@code tasks.task.payload} and back. */
public interface PayloadCodec<T> {

    String encode(T payload);

    T decode(String data);

    /** Stored in {@code payload_type}, for versioning. {@code null} by default. */
    default @Nullable String type() {
        return null;
    }

    static PayloadCodec<String> string() {
        return of(Function.identity(), Function.identity());
    }

    static <T> PayloadCodec<T> of(Function<T, String> encoder, Function<String, T> decoder) {
        Objects.requireNonNull(encoder, "encoder");
        Objects.requireNonNull(decoder, "decoder");
        return new PayloadCodec<>() {
            @Override
            public String encode(T payload) {
                return encoder.apply(payload);
            }

            @Override
            public T decode(String data) {
                return decoder.apply(data);
            }
        };
    }
}
