package org.example.task2.admin;

/** @param result failed or expired */
public record AttemptView(long taskId, int attempt, Integer nodeId, String result, String error) {
}
