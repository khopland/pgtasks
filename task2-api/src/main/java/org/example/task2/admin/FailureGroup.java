package org.example.task2.admin;

import java.time.Instant;

/** @param example the lowest task id in the group, to look at */
public record FailureGroup(String error, long tasks, Instant oldestFailedAt, long example, String exampleMessage) {
}
