package io.github.khopland.pgtasks;

import java.util.Base64;
import java.util.Objects;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

/**
 * Turns payloads into the text stored in the task table's {@code payload} column and back. Binary
 * formats, such as Protobuf or Avro, go through {@link #binary}.
 */
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

    /**
     * A codec for a binary format, stored as Base64 text (RFC 4648, with padding): a third larger
     * than the bytes, and opaque in SQL and in the endpoint, but every store, view and tool reads
     * it as it reads any payload. For instance {@code PayloadCodec.binary(Invoice::toByteArray, Invoice::parseFrom)}
     * with a parser that throws no checked exception. Give it a {@link #type()} by wrapping it if
     * the format is versioned.
     */
    static <T> PayloadCodec<T> binary(Function<T, byte[]> encoder, Function<byte[], T> decoder) {
        Objects.requireNonNull(encoder, "encoder");
        Objects.requireNonNull(decoder, "decoder");
        return of(payload -> Base64.getEncoder().encodeToString(encoder.apply(payload)), data -> decoder.apply(Base64.getDecoder().decode(data)));
    }

    /** Byte arrays as they are, stored as Base64 text; see {@link #binary}. */
    static PayloadCodec<byte[]> bytes() {
        return binary(Function.identity(), Function.identity());
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
