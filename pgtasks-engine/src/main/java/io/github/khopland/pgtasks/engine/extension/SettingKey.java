package io.github.khopland.pgtasks.engine.extension;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import io.github.khopland.pgtasks.engine.TopicOptions;

import org.jspecify.annotations.Nullable;

/**
 * A setting an {@link Extension} adds, such as {@code window.size}. A per-topic one, declared in
 * {@link Extension#settings()}, layers like the engine's own: the key's default, then global and
 * module defaults, the topic's options ({@link TopicOptions#set}), and overrides
 * ({@code topics.<topic>.<name>}, parsed with {@link #parse}); the extension reads it with
 * {@link ExtensionContext#topicSetting}. An engine-wide one, declared in
 * {@link Extension#engineSettings()}, has the key's default, what the builder sets and an
 * override ({@code extensions.<name>}); the extension reads it with {@link ExtensionContext#setting}.
 * Either shows in the effective settings with where its value came from.
 *
 * <p>A {@linkplain #checking check} applies to every value, whichever layer it comes from: the
 * default, a value set in code, and an override. The check of the key the extension declares
 * applies, whichever key a value is set with.
 *
 * <p>Two keys are equal when they have the same name, type and default: they are one setting,
 * whatever their parsers and checks.
 */
public final class SettingKey<T> {

    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9-]*(\\.[a-z][a-z0-9-]*)+");

    private final String name;
    private final Class<T> type;
    private final T defaultValue;
    private final Function<String, ? extends T> parser;
    private final Consumer<? super T> check;

    /**
     * A key that takes every value of its type; see {@link #of}.
     *
     * @param name         a dotted lowercase name, such as {@code window.size}; the dot keeps it apart
     *                     from the engine's own settings
     * @param type         the value's type, checked when a value is set
     * @param defaultValue the value when nothing sets it
     * @param parser       reads an override's text; throws IllegalArgumentException for a bad one
     */
    public SettingKey(String name, Class<T> type, T defaultValue, Function<String, ? extends T> parser) {
        this(name, type, defaultValue, parser, SettingKey::any);
    }

    private SettingKey(String name, Class<T> type, T defaultValue, Function<String, ? extends T> parser, Consumer<? super T> check) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(defaultValue, "defaultValue");
        Objects.requireNonNull(parser, "parser");
        Objects.requireNonNull(check, "check");
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid setting name '" + name + "': dotted lowercase words, such as window.size");
        }
        if (!type.isInstance(defaultValue)) {
            throw new IllegalArgumentException("The default of " + name + " is not a " + type.getName());
        }
        try {
            check.accept(defaultValue);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("The default of " + name + " is invalid: " + e.getMessage(), e);
        }
        this.name = name;
        this.type = type;
        this.defaultValue = defaultValue;
        this.parser = parser;
        this.check = check;
    }

    public static <T> SettingKey<T> of(String name, Class<T> type, T defaultValue, Function<String, ? extends T> parser) {
        return new SettingKey<>(name, type, defaultValue, parser);
    }

    /**
     * This key, taking only values the check accepts as well as those its checks so far accept:
     * the check throws IllegalArgumentException, with a message saying why, for one it doesn't.
     * Applied to the default here, and to every value set in code or overridden.
     */
    public SettingKey<T> checking(Consumer<? super T> check) {
        Objects.requireNonNull(check, "check");
        Consumer<? super T> before = this.check;
        Consumer<T> both = value -> {
            before.accept(value);
            check.accept(value);
        };
        return new SettingKey<>(name, type, defaultValue, parser, both);
    }

    /** A dotted lowercase name, such as {@code window.size}. */
    public String name() {
        return name;
    }

    /** The value's type. */
    public Class<T> type() {
        return type;
    }

    /** The value when nothing sets it. */
    public T defaultValue() {
        return defaultValue;
    }

    /** Reads an override's text, before the check; see {@link #parse}. */
    public Function<String, ? extends T> parser() {
        return parser;
    }

    /**
     * The value of an override's text.
     *
     * @throws IllegalArgumentException saying why, if the parser can't read it or the check refuses it
     */
    public T parse(String text) {
        T value = parser.apply(text.trim());
        if (!type.isInstance(value)) {
            throw new IllegalArgumentException("not a " + type.getSimpleName() + ": " + text);
        }
        check.accept(value);
        return value;
    }

    /**
     * The value, if it is one of this setting's type and passes its check.
     *
     * @throws IllegalArgumentException naming the setting, if it isn't
     */
    public T checked(Object value) {
        if (!type.isInstance(value)) {
            throw new IllegalArgumentException(name + " takes a " + type.getSimpleName() + ", not " + value);
        }
        T typed = type.cast(value);
        try {
            check.accept(typed);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(name + ": " + e.getMessage(), e);
        }
        return typed;
    }

    /** The check of a key that takes every value. */
    private static void any(Object value) {
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof SettingKey<?> key && key.name.equals(name) && key.type.equals(type) && key.defaultValue.equals(defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, defaultValue);
    }

    @Override
    public String toString() {
        return "SettingKey[" + name + ": " + type.getSimpleName() + " = " + defaultValue + "]";
    }
}
