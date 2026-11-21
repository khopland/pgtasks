package io.github.khopland.pgtasks.engine;

import java.io.PrintWriter;
import java.io.StringWriter;

/** The structured error stored in tasks.task.error and tasks.attempt.error. */
final class ErrorJson {

    private static final int MAX_STACK = 4000;

    private ErrorJson() {
    }

    static String of(String message, Throwable cause) {
        StringBuilder json = new StringBuilder("{");
        field(json, "message", message != null ? message : cause != null ? cause.toString() : "failed");
        if (cause != null) {
            json.append(',');
            field(json, "type", cause.getClass().getName());
            StringWriter stack = new StringWriter();
            cause.printStackTrace(new PrintWriter(stack));
            String trace = stack.toString();
            json.append(',');
            field(json, "stack", trace.length() > MAX_STACK ? trace.substring(0, MAX_STACK) + "..." : trace);
        }
        return json.append('}').toString();
    }

    private static void field(StringBuilder json, String name, String value) {
        json.append('"').append(name).append("\":\"");
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                default -> {
                    if (character < 0x20) {
                        json.append(String.format("\\u%04x", (int) character));
                    } else {
                        json.append(character);
                    }
                }
            }
        }
        json.append('"');
    }
}
