package io.github.khopland.pgtasks;

import java.util.Locale;

/**
 * How a store treats its schema at startup: {@code TaskEngineBuilder.schemaMode}, the Spring
 * property {@code pgtasks.schema-mode}, or the Postgres store's constructor.
 */
public enum SchemaMode {

    /**
     * The built-in runner applies the SQL files, then checks the version. The default. On
     * Postgres it fails fast on a server older than the oldest one supported, before applying
     * anything.
     */
    MIGRATE,

    /**
     * Only checks the version recorded in the schema's {@code meta} table, for when Flyway,
     * Liquibase or a DBA pipeline applies the same SQL files. Fails fast with a message naming the
     * options, and on a server older than the oldest one supported.
     */
    VALIDATE,

    /** Does nothing, not even the version checks. */
    NONE;

    /** As written in configuration: {@code migrate}, {@code validate} or {@code none}. */
    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** From its {@link #label()}, in any case. */
    public static SchemaMode of(String label) {
        for (SchemaMode mode : values()) {
            if (mode.name().equalsIgnoreCase(label.strip())) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown schema mode '" + label + "': use migrate, validate or none");
    }
}
