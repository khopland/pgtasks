package org.example.task2.spring;

import org.example.task2.PayloadCodec;
import org.example.task2.engine.PayloadCodecs;
import tools.jackson.databind.json.JsonMapper;

/**
 * JSON for every payload type without a codec of its own, with the application's Jackson
 * mapper, so a topic of records needs no codec. Stored with payload type {@code json}.
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
