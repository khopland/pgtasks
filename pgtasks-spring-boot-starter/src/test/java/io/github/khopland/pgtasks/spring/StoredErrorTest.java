package io.github.khopland.pgtasks.spring;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StoredErrorTest {

    @Test
    void an_engine_error_becomes_an_object_with_type_message_and_stack_first() {
        // As Postgres returns jsonb: keys by length, then by name, with a space after each colon.
        String stored = "{\"type\": \"java.net.ConnectException\", \"stack\": \"java.net.ConnectException: refused\\n\\tat A.b(A.java:1)\", "
            + "\"message\": \"refused \\\"partner\\\" \\u00e6\\/\"}";

        Map<String, Object> error = StoredError.parse(stored);

        assertThat(error).containsExactly(
            Map.entry("type", "java.net.ConnectException"),
            Map.entry("message", "refused \"partner\" æ/"),
            Map.entry("stack", "java.net.ConnectException: refused\n\tat A.b(A.java:1)"));
    }

    @Test
    void anything_but_a_flat_object_of_strings_is_its_message() {
        assertThat(StoredError.parse("failed for good")).isEqualTo(Map.of("message", "failed for good"));
        assertThat(StoredError.parse("{\"attempts\": 3}")).isEqualTo(Map.of("message", "{\"attempts\": 3}"));
        assertThat(StoredError.parse("{\"message\": \"cut off")).isEqualTo(Map.of("message", "{\"message\": \"cut off"));
        assertThat(StoredError.parse("{\"message\": \"a\"} trailing")).isEqualTo(Map.of("message", "{\"message\": \"a\"} trailing"));
        assertThat(StoredError.parse("{}")).isEmpty();
    }

    @Test
    void hidden_an_error_keeps_only_its_type() {
        assertThat(StoredError.typeOnly("{\"type\": \"x.Y\", \"message\": \"secret\"}", "***"))
            .isEqualTo(Map.of("type", "x.Y", "message", "***"));
        assertThat(StoredError.typeOnly("{\"message\": \"secret\"}", "***")).isEqualTo("***");
        assertThat(StoredError.typeOnly("secret", "***")).isEqualTo("***");
    }
}
