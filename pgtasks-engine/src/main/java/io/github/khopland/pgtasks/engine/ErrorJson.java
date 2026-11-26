package io.github.khopland.pgtasks.engine;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.jspecify.annotations.Nullable;

/**
 * The structured error stored in pgtasks.task.error.
 *
 * <p>Never fails: the error is written in the completion of the task's batch, so a failure here
 * would fail every task of the batch, and an exception thrown before the completion would leave
 * the batch to the orphan sweep, which hands it back without counting the attempt, forever. So an
 * exception whose {@code toString} or stack trace throws is described by its class instead, and a
 * NUL character, which Postgres can't store in text or jsonb, becomes U+FFFD.
 *
 * <p>The message is cut at {@link #MAX_MESSAGE} characters and the stack at {@link #MAX_STACK}: an
 * exception that carries a whole response body, as HTTP clients' do, would otherwise store
 * megabytes per failure, on every retry, in the completion of the whole batch.
 */
final class ErrorJson {

    private static final int MAX_STACK = 4000;
    static final int MAX_MESSAGE = 8000;
    private static final char REPLACEMENT = '�';

    private ErrorJson() {
    }

    static String of(@Nullable String message, @Nullable Throwable cause) {
        StringBuilder json = new StringBuilder("{");
        field(json, "message", cut(message != null ? message : cause != null ? describe(cause) : "failed", MAX_MESSAGE));
        if (cause != null) {
            json.append(',');
            field(json, "type", cause.getClass().getName());
            String trace = stack(cause);
            json.append(',');
            field(json, "stack", cut(trace, MAX_STACK));
        }
        return json.append('}').toString();
    }

    /** An error the engine describes itself, with a type of its own for grouping, such as {@code expired}. */
    static String typed(String message, String type) {
        StringBuilder json = new StringBuilder("{");
        field(json, "message", cut(message, MAX_MESSAGE));
        json.append(',');
        field(json, "type", type);
        return json.append('}').toString();
    }

    /**
     * The text, or its first {@code max} characters and a note of how many more there were. Never
     * splits a surrogate pair, which would leave half a character Postgres can't encode.
     */
    static String cut(String text, int max) {
        if (text.length() <= max) {
            return text;
        }
        int end = Character.isHighSurrogate(text.charAt(max - 1)) ? max - 1 : max;
        return text.substring(0, end) + "... (" + (text.length() - end) + " more characters)";
    }

    /** The cause's {@code toString}, or its class name if that throws. */
    private static String describe(Throwable cause) {
        try {
            return String.valueOf(cause);
        } catch (RuntimeException | LinkageError | StackOverflowError e) {
            return cause.getClass().getName() + " (its toString() threw " + e.getClass().getName() + ")";
        }
    }

    /** The cause's stack trace, or what could be written of it before printing it threw. */
    private static String stack(Throwable cause) {
        StringWriter stack = new StringWriter();
        try {
            cause.printStackTrace(new PrintWriter(stack));
        } catch (RuntimeException | LinkageError | StackOverflowError e) {
            stack.append(System.lineSeparator()).append("(printing the stack trace threw ").append(e.getClass().getName()).append(')');
        }
        return stack.toString();
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
                case '\0' -> json.append(REPLACEMENT);
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
