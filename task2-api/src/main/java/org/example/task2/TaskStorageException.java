package org.example.task2;

import java.sql.SQLException;

/** A push or another call into task storage failed in the database. */
public class TaskStorageException extends RuntimeException {

    public TaskStorageException(String message, SQLException cause) {
        super(message, cause);
    }
}
