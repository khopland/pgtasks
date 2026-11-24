package io.github.khopland.pgtasks.engine;

import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * A per-topic setting an {@link Extension} adds, such as {@code window.size}. It layers like the
 * engine's own: the key's default, then global and module defaults, the topic's options
 * ({@link TopicOptions#set}), and overrides ({@code topics.<topic>.<name>}, parsed with
 * {@link #parse}). It shows in the effective settings with where its value came from. The
 * extension declares it in {@link Extension#settings()} and reads it with
 * {@link ExtensionContext#topicSetting}.
 *
 * @param name         a dotted lowercase name, such as {@code window.size}; the dot keeps it apart
 *                     from the engine's own settings
 * @param type         the value's type, checked when a value is set
 * @param defaultValue the value of a topic that doesn't set it
 * @param parser       reads an override's text; throws IllegalArgumentException for a bad one
 */
public record SettingKey<T>(String name, Class<T> type, T defaultValue, Function<String, ? extends T> parser) {

    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9-]*(\\.[a-z][a-z0-9-]*)+");

    public SettingKey {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(defaultValue, "defaultValue");
        Objects.requireNonNull(parser, "parser");
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid setting name '" + name + "': dotted lowercase words, such as window.size");
        }
        if (!type.isInstance(defaultValue)) {
            throw new IllegalArgumentException("The default of " + name + " is not a " + type.getName());
        }
    }

    public static <T> SettingKey<T> of(String name, Class<T> type, T defaultValue, Function<String, ? extends T> parser) {
        return new SettingKey<>(name, type, defaultValue, parser);
    }

    /** The value of an override's text. */
    public T parse(String text) {
        return checked(parser.apply(text.trim()));
    }

    /** The value, if it is one of this setting's type. */
    T checked(Object value) {
        if (!type.isInstance(value)) {
            throw new IllegalArgumentException(name + " takes a " + type.getSimpleName() + ", not " + value);
        }
        return type.cast(value);
    }
}
