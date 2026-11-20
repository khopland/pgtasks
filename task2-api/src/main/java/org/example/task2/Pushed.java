package org.example.task2;

import java.util.OptionalLong;

/**
 * The result of pushing one creation.
 *
 * @param identifier the creation's identifier
 * @param status     what happened to it
 * @param id         the new task's id; absent for a {@link Status#DUPLICATE}
 */
public record Pushed(String identifier, Status status, Long id) {

    public enum Status {
        /** Stored and queued. */
        QUEUED,
        /** Stored as redundant, because a later creation in the same push has the same identifier. */
        REDUNDANT,
        /** Not stored, because an unfinished task with the same dedup key already exists. */
        DUPLICATE
    }

    public OptionalLong findId() {
        return id == null ? OptionalLong.empty() : OptionalLong.of(id);
    }
}
