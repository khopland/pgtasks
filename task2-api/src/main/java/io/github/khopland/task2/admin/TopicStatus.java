package io.github.khopland.task2.admin;

import java.time.Instant;

import org.jspecify.annotations.Nullable;

/**
 * One topic as an operator sees it: registered in the store, paused or not, processed on this
 * node or not, which module registered it here, and its last snapshotted counts.
 *
 * @param processing  whether this node processes it now
 * @param module      the configurer that registered the topic on this node, or null
 * @param counts      from the last snapshot, or null if none was taken yet
 * @param countedAt   when that snapshot was taken, or null
 * @param breakerOpen whether this node's breaker for the topic is open
 */
public record TopicStatus(String name, boolean paused, boolean processing, @Nullable String module, @Nullable QueueCounts counts,
                          @Nullable Instant countedAt,
                          boolean breakerOpen) {
}
