package io.github.khopland.pgtasks.postgres;

/** The tasks schema is missing, too old, incompatible, or a migration failed. */
public class TaskSchemaException extends RuntimeException {

    public TaskSchemaException(String message) {
        super(message);
    }

    public TaskSchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
