package org.example.task2.engine;

/** A change named the version it was based on, and someone else changed the thing since. */
public final class VersionConflictException extends RuntimeException {

    public VersionConflictException(String message) {
        super(message);
    }
}
