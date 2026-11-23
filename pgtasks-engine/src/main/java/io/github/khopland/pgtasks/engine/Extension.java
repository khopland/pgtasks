package io.github.khopland.pgtasks.engine;

/**
 * A feature added to the engine from outside its core, through the hooks of
 * {@link ExtensionContext}. Register one with {@link TaskEngineBuilder#extension}, or, with
 * Spring, as a bean. Each is installed once, when the engine is built, before it is prepared or
 * started.
 */
public interface Extension {

    /** For logs and errors; the class's simple name unless overridden. */
    default String name() {
        return getClass().getSimpleName();
    }

    /** Registers the extension's hooks. Called once, while the engine is built. */
    void install(ExtensionContext context);
}
