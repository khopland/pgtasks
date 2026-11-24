package io.github.khopland.pgtasks.postgres;

/** The pgtasks schema is missing, too old, incompatible, or a migration failed; or the server's Postgres is older than pgtasks supports. */
public class TaskSchemaException extends RuntimeException {

    public TaskSchemaException(String message) {
        super(message);
    }

    public TaskSchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
