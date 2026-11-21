package io.github.khopland.pgtasks.admin;

import java.time.Instant;

public record TopicSnapshot(String topic, QueueCounts counts, Instant takenAt) {
}
