package io.github.khopland.pgtasks.postgres;

/** How the engine treats the pgtasks schema at startup. */
public enum SchemaMode {

    /**
     * The built-in runner applies the SQL files, then checks the version. The default. Fails fast on
     * a server older than {@link TaskSchema#MIN_POSTGRES_VERSION}, before applying anything.
     */
    MIGRATE,

    /**
     * Only checks the version recorded in the schema's {@code meta} table, for when Flyway, Liquibase or a DBA
     * pipeline applies the same SQL files. Fails fast with a message naming the options, and on a
     * server older than {@link TaskSchema#MIN_POSTGRES_VERSION}.
     */
    VALIDATE,

    /** Does nothing, not even the version checks. */
    NONE
}
