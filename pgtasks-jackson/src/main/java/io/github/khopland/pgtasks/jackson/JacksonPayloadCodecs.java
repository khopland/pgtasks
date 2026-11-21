package io.github.khopland.pgtasks.jackson;

import io.github.khopland.pgtasks.PayloadCodec;
import io.github.khopland.pgtasks.engine.PayloadCodecs;
import tools.jackson.databind.json.JsonMapper;

/**
 * JSON for every payload type without a codec of its own, so a topic of records needs no codec.
 * Stored with payload type {@code json}. The Spring starter gives one with the application's
 * mapper; without Spring:
 *
 * <pre>{@code
 * TaskEngine.builder()
 *     .dataSource(dataSource)
 *     .codecs(new JacksonPayloadCodecs(JsonMapper.builder().build()))
 *     ...
 * }</pre>
 */
public class JacksonPayloadCodecs implements PayloadCodecs {

    private final JsonMapper mapper;

    public JacksonPayloadCodecs(JsonMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public PayloadCodec<?> codecFor(Class<?> type) {
        return codec(type);
    }

    private <T> PayloadCodec<T> codec(Class<T> type) {
        return new PayloadCodec<>() {
            @Override
            public String encode(T payload) {
                return mapper.writeValueAsString(payload);
            }

            @Override
            public T decode(String data) {
                return mapper.readValue(data, type);
            }

            @Override
            public String type() {
                return "json";
            }
        };
    }
}
