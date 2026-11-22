package io.github.khopland.pgtasks.postgres;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * A task's headers as the {@code jsonb} object of strings they are stored as. Reads the object
 * back the way Postgres prints it, or any other JSON object: a string, number or boolean value is
 * kept as its text, a null one is left out, and anything nested is refused.
 */
final class HeaderJson {

    private final String json;
    private int at;

    private HeaderJson(String json) {
        this.json = json;
    }

    /** The headers as a JSON object, or null for none, so a task without headers stores NULL. */
    static @Nullable String write(Map<String, String> headers) {
        if (headers.isEmpty()) {
            return null;
        }
        StringBuilder json = new StringBuilder("{");
        headers.forEach((name, value) -> {
            if (json.length() > 1) {
                json.append(',');
            }
            string(json, name);
            json.append(':');
            string(json, value);
        });
        return json.append('}').toString();
    }

    static Map<String, String> read(@Nullable String json) {
        if (json == null) {
            return Map.of();
        }
        return new HeaderJson(json).object();
    }

    private static void string(StringBuilder json, String value) {
        json.append('"');
        for (int index = 0; index < value.length(); index++) {
            char c = value.charAt(index);
            switch (c) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                default -> {
                    if (c < 0x20) {
                        json.append(String.format("\\u%04x", (int) c));
                    } else {
                        json.append(c);
                    }
                }
            }
        }
        json.append('"');
    }

    private Map<String, String> object() {
        Map<String, String> headers = new LinkedHashMap<>();
        expect('{');
        if (peek() == '}') {
            at++;
        } else {
            do {
                String name = string();
                expect(':');
                String value = value();
                if (value != null) {
                    headers.put(name, value);
                }
            } while (next() == ',');
            at--;
            expect('}');
        }
        if (peek() != 0) {
            throw invalid("text after the object");
        }
        return Collections.unmodifiableMap(headers);
    }

    private @Nullable String value() {
        char c = peek();
        if (c == '"') {
            return string();
        }
        if (c == '{' || c == '[') {
            throw invalid("a nested value");
        }
        int start = at;
        while (at < json.length() && ",}".indexOf(json.charAt(at)) < 0 && !Character.isWhitespace(json.charAt(at))) {
            at++;
        }
        String literal = json.substring(start, at);
        if (literal.isEmpty()) {
            throw invalid("a missing value");
        }
        return literal.equals("null") ? null : literal;
    }

    private String string() {
        expect('"');
        StringBuilder value = new StringBuilder();
        while (true) {
            if (at >= json.length()) {
                throw invalid("an unterminated string");
            }
            char c = json.charAt(at++);
            if (c == '"') {
                return value.toString();
            }
            if (c != '\\') {
                value.append(c);
                continue;
            }
            if (at >= json.length()) {
                throw invalid("an unterminated string");
            }
            char escaped = json.charAt(at++);
            switch (escaped) {
                case '"', '\\', '/' -> value.append(escaped);
                case 'b' -> value.append('\b');
                case 'f' -> value.append('\f');
                case 'n' -> value.append('\n');
                case 'r' -> value.append('\r');
                case 't' -> value.append('\t');
                case 'u' -> {
                    if (at + 4 > json.length()) {
                        throw invalid("a short unicode escape");
                    }
                    try {
                        value.append((char) Integer.parseInt(json.substring(at, at + 4), 16));
                    } catch (NumberFormatException e) {
                        throw invalid("a bad unicode escape");
                    }
                    at += 4;
                }
                default -> throw invalid("an unknown escape");
            }
        }
    }

    private void expect(char expected) {
        if (next() != expected) {
            throw invalid("no '" + expected + "'");
        }
    }

    /** The next character that isn't whitespace, consumed; 0 at the end. */
    private char next() {
        char c = peek();
        if (c != 0) {
            at++;
        }
        return c;
    }

    /** The next character that isn't whitespace, not consumed; 0 at the end. */
    private char peek() {
        while (at < json.length() && Character.isWhitespace(json.charAt(at))) {
            at++;
        }
        return at < json.length() ? json.charAt(at) : 0;
    }

    private IllegalStateException invalid(String what) {
        return new IllegalStateException("Stored task headers are not a JSON object of strings: " + what + " at " + at + " in " + json);
    }
}
