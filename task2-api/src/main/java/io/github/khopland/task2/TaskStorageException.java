package io.github.khopland.task2;

import java.sql.SQLException;

/**
 * A push, or another call into task storage, failed: the database refused or couldn't be
 * reached. Unchecked, like the rest of the library's failures; the cause says more.
 */
public class TaskStorageException extends RuntimeException {

    public TaskStorageException(String message, SQLException cause) {
        super(message, cause);
    }

    public TaskStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
