package org.example.task2.admin;

import java.time.Instant;

public record TopicSnapshot(String topic, QueueCounts counts, Instant takenAt) {
}
