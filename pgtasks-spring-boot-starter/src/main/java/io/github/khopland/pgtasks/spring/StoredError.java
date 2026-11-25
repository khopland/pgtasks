package io.github.khopland.pgtasks.spring;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A task's stored error as the endpoint shows it: an object, not JSON inside a string. The engine
 * stores a flat JSON object of strings ({@code type}, {@code message}, {@code stack}); anything
 * else, such as a plain text from a 1.x import, is shown as its {@code message}. Reading it needs
 * no JSON library, so the endpoint works without Jackson.
 */
final class StoredError {

    /** The fields operators read first, in this order; any others follow as stored. */
    private static final List<String> FIRST = List.of("type", "message", "stack");

    private final String text;
    private int at;

    private StoredError(String text) {
        this.text = text;
    }

    static Map<String, Object> parse(String text) {
        Map<String, Object> fields;
        try {
            fields = new StoredError(text).object();
        } catch (IllegalArgumentException | StringIndexOutOfBoundsException e) {
            return Map.of("message", text);
        }
        Map<String, Object> ordered = new LinkedHashMap<>();
        FIRST.stream().filter(fields::containsKey).forEach(name -> ordered.put(name, fields.get(name)));
        ordered.putAll(fields);
        return ordered;
    }

    /** Only the exception type, the message hidden: {@code {"type": ..., "message": hidden}}, or hidden without a type. */
    static Object typeOnly(String text, String hidden) {
        Object type = parse(text).get("type");
        if (type instanceof String name) {
            Map<String, Object> shown = new LinkedHashMap<>();
            shown.put("type", name);
            shown.put("message", hidden);
            return shown;
        }
        return hidden;
    }

    private Map<String, Object> object() {
        Map<String, Object> fields = new LinkedHashMap<>();
        expect('{');
        if (peek() == '}') {
            at++;
        } else {
            while (true) {
                String name = string();
                expect(':');
                fields.put(name, string());
                char next = next();
                if (next == '}') {
                    break;
                }
                if (next != ',') {
                    throw new IllegalArgumentException("expected , or }");
                }
            }
        }
        whitespace();
        if (at != text.length()) {
            throw new IllegalArgumentException("text after the object");
        }
        return fields;
    }

    private String string() {
        expect('"');
        StringBuilder value = new StringBuilder();
        while (true) {
            char c = text.charAt(at++);
            if (c == '"') {
                return value.toString();
            }
            if (c != '\\') {
                value.append(c);
                continue;
            }
            char escaped = text.charAt(at++);
            switch (escaped) {
                case '"', '\\', '/' -> value.append(escaped);
                case 'b' -> value.append('\b');
                case 'f' -> value.append('\f');
                case 'n' -> value.append('\n');
                case 'r' -> value.append('\r');
                case 't' -> value.append('\t');
                case 'u' -> {
                    value.append((char) Integer.parseInt(text.substring(at, at + 4), 16));
                    at += 4;
                }
                default -> throw new IllegalArgumentException("bad escape \\" + escaped);
            }
        }
    }

    private void expect(char wanted) {
        if (next() != wanted) {
            throw new IllegalArgumentException("expected " + wanted);
        }
    }

    /** The next character that isn't whitespace, consumed. */
    private char next() {
        whitespace();
        return text.charAt(at++);
    }

    private char peek() {
        whitespace();
        return text.charAt(at);
    }

    private void whitespace() {
        while (at < text.length() && Character.isWhitespace(text.charAt(at))) {
            at++;
        }
    }
}
