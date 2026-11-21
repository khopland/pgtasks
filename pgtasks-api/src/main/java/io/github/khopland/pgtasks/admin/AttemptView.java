package io.github.khopland.pgtasks.admin;

import org.jspecify.annotations.Nullable;

/** @param result failed or expired */
public record AttemptView(long taskId, int attempt, @Nullable Integer nodeId, String result, @Nullable String error) {
}
