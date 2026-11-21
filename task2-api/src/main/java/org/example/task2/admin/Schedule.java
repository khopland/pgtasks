package org.example.task2.admin;

import java.time.Instant;

import org.example.task2.Insertion;

/** A stored recurring task. */
public record Schedule(int id, int topicId, String topic, String name, String identifier, String cron, String zone, Insertion insertion,
                String payload, String payloadType, Instant nextRunAt, Instant lastRunAt) {
}
