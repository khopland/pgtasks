package org.example.task2;

import java.util.OptionalLong;

/**
 * The result of pushing one creation.
 *
 * @param identifier the creation's identifier
 * @param status     what happened to it
 * @param id         the new task's id; absent for a {@link Status#DUPLICATE} or {@link Status#PENDING}
 */
public record Pushed(String identifier, Status status, Long id) {

    public enum Status {
        /** Stored and queued. */
        QUEUED,
        /** Stored as redundant, because a later creation in the same push has the same identifier. */
        REDUNDANT,
        /** Not stored, because an unfinished task with the same dedup key already exists. */
        DUPLICATE,
        /**
         * Held back until the surrounding transaction commits, and dropped if it rolls back. Happens
         * with a store that keeps its tasks outside the application's database, such as the
         * in-memory store; the outcome is only known after the commit.
         */
        PENDING
    }

    public OptionalLong findId() {
        return id == null ? OptionalLong.empty() : OptionalLong.of(id);
    }
}
