package org.example.task2.admin;

import java.time.Instant;

import org.example.task2.Insertion;

import org.jspecify.annotations.Nullable;

/** A stored recurring task. */
public record Schedule(int id, int topicId, String topic, String name, String identifier, String cron, String zone, Insertion insertion,
                @Nullable String payload, @Nullable String payloadType, Instant nextRunAt, @Nullable Instant lastRunAt) {
}
