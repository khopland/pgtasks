package io.github.khopland.task2.postgres;

/** How the engine treats the tasks schema at startup. */
public enum SchemaMode {

    /** The built-in runner applies the SQL files, then checks the version. The default. */
    MIGRATE,

    /**
     * Only checks the version recorded in {@code tasks.meta}, for when Flyway, Liquibase or a DBA
     * pipeline applies the same SQL files. Fails fast with a message naming the options.
     */
    VALIDATE,

    /** Does nothing. */
    NONE
}
