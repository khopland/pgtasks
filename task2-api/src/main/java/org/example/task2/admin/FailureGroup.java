package org.example.task2.admin;

import java.time.Instant;

import org.jspecify.annotations.Nullable;

/** @param example the lowest task id in the group, to look at */
public record FailureGroup(String error, long tasks, Instant oldestFailedAt, long example, @Nullable String exampleMessage) {
}
