package io.github.khopland.pgtasks.engine.extension;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import io.github.khopland.pgtasks.engine.TaskEngineBuilder;

/**
 * A feature added to the engine from outside its core, through the hooks of
 * {@link ExtensionContext}. Register one with {@link TaskEngineBuilder#extension}, or, with
 * Spring, as a bean. Each is installed once per engine, when the engine is built, before it is
 * prepared or started.
 *
 * <p>One instance may be installed in several engines, such as one per test or several nodes of a
 * {@code TaskTester} on one store: {@link #install} is called once for each, with that engine's
 * context. Keep what belongs to one engine in what {@code install} creates, not in the instance's
 * own fields. Two extensions with the same {@link #name()} can't be installed in one engine, since
 * installing a feature twice usually means it counts twice.
 */
public interface Extension {

    /** For logs, errors and the starter's views; the class's simple name unless overridden. Unique per engine. */
    default String name() {
        return getClass().getSimpleName();
    }

    /** The per-topic settings it adds, so they get defaults and overrides; see {@link SettingKey}. */
    default List<SettingKey<?>> settings() {
        return List.of();
    }

    /**
     * The engine-wide settings it adds, such as how often it looks for work: set in code with
     * {@link TaskEngineBuilder#set}, overridden as {@code extensions.<name>} (in Spring,
     * {@code pgtasks.extensions.<name>}), and read with {@link ExtensionContext#setting}.
     */
    default List<SettingKey<?>> engineSettings() {
        return List.of();
    }

    /** Registers the extension's hooks. Called once per engine, while the engine is built. */
    void install(ExtensionContext context);

    /**
     * A small extension from a name and what it installs, such as a single hook:
     * {@code Extension.of("audit", context -> context.batchListener(audit::record))}.
     */
    static Extension of(String name, Consumer<ExtensionContext> install) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(install, "install");
        return new Extension() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public void install(ExtensionContext context) {
                install.accept(context);
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }
}
