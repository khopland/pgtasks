package org.example.task2;

/** How a finished task ended, as recorded in tasks.task. */
public enum Outcome {
    SUCCEEDED, FILTERED, FAILED, REDUNDANT, RECREATED;

    public String label() {
        return name().toLowerCase(java.util.Locale.ROOT);
    }
}
